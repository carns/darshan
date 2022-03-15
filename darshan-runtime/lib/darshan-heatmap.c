/*
 * Copyright (C) 2021 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include <stdint.h>
#ifdef HAVE_STDATOMIC_H
#include <stdatomic.h>
#endif

#include "darshan.h"
#include "darshan-heatmap.h"

/* maximum number of bins per record */
/* TODO: make this tunable at runtime */
/* TODO: safety check that total record size plus trailing bins doesn't
 * exceed DEF_MOD_BUF_SIZE.  If it does, the log will still be technically
 * valid but the default darshan-parser will not be able to display it.
 */
#define DARSHAN_MAX_HEATMAP_BINS 200

/* initial width of each bin, as floating point seconds */
/* TODO: make this tunable at runtime */
#define DARSHAN_INITIAL_BIN_WIDTH_SECONDS 0.1

/* maximum number of distinct heatmaps that we will track (there is a
 * heatmap per module that interacts with it, not per file, so we should not
 * need many).  If this limit is exceeded then the darshan core will mark
 * the "partial" flag for the log so that we will be able to tell that the
 * limit has been hit.
 */
/* TODO: make this tunable at runtime */
#define DARSHAN_MAX_HEATMAPS 8

/* structure to track heatmaps at runtime */
struct heatmap_record_ref
{
    struct darshan_heatmap_record* heatmap_rec;
};

/* The heatmap_runtime structure maintains necessary state for storing
 * heatmap records and for coordinating with darshan-core at shutdown time.
 */
struct heatmap_runtime
{
    void *rec_id_hash;
    int rec_count;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

static struct heatmap_runtime *heatmap_runtime = NULL;
static int my_rank = -1;

static struct heatmap_record_ref *heatmap_track_new_record(
    darshan_record_id rec_id, const char *name);

#ifdef HAVE_STDATOMIC_H
atomic_flag heatmap_runtime_mutex;
#define HEATMAP_LOCK() \
    while (atomic_flag_test_and_set(&heatmap_runtime_mutex))
#define HEATMAP_UNLOCK() \
    atomic_flag_clear(&heatmap_runtime_mutex)
#else
static pthread_mutex_t heatmap_runtime_mutex = PTHREAD_MUTEX_INITIALIZER;
#define HEATMAP_LOCK() pthread_mutex_lock(&heatmap_runtime_mutex)
#define HEATMAP_UNLOCK() pthread_mutex_unlock(&heatmap_runtime_mutex)
#endif

/* note that if the break condition is triggered in this macro, then it
 * will exit the do/while loop holding a lock that will be released in
 * POST_RECORD().  Otherwise it will release the lock here (if held) and
 * return immediately without reaching the POST_RECORD() macro.
 */
/* NOTE: unlike other modules, the PRE_RECORD here does not attempt to
 * initialize this module if it isn't already.  That should have been done
 * in the _register() call before reaching this point.  Skipping the
 * initialization attempt here makes it safe to use atomics or spinlocks
 * in the critical wrapper path.
 */
#define HEATMAP_PRE_RECORD() do { \
    HEATMAP_LOCK(); \
    if(heatmap_runtime && !heatmap_runtime->frozen) break; \
    HEATMAP_UNLOCK(); \
    return(ret); \
} while(0)

/* same as above but for void fns */
#define HEATMAP_PRE_RECORD_VOID() do { \
    HEATMAP_LOCK(); \
    if(heatmap_runtime && !heatmap_runtime->frozen) break; \
    HEATMAP_UNLOCK(); \
    return; \
} while(0)

#define HEATMAP_POST_RECORD() do { \
    HEATMAP_UNLOCK(); \
} while(0)

static void heatmap_output(
    void **heatmap_buf,
    int *heatmap_buf_sz)
{
    struct darshan_heatmap_record* rec;
    void* contig_buf_ptr;
    int i;
    double end_timestamp;
    unsigned long this_size;
    int tmp_nbins;

    HEATMAP_LOCK();
    assert(heatmap_runtime);

    *heatmap_buf_sz = 0;

    heatmap_runtime->frozen = 1;
    end_timestamp = darshan_core_wtime();

    contig_buf_ptr = *heatmap_buf;

    /* iterate through records (heatmap histograms) */
    for(i=0; i<heatmap_runtime->rec_count; i++)
    {
        rec = (struct darshan_heatmap_record*)((uintptr_t)*heatmap_buf + i*(sizeof(*rec) + DARSHAN_MAX_HEATMAP_BINS*2*sizeof(int64_t)));

        tmp_nbins= ceil(end_timestamp/rec->bin_width_seconds);

        /* are there bins beyond the execution time of the program? */
        if(tmp_nbins < rec->nbins)
        {
            /* truncate bins so that we don't report any beyond the time when
             * instrumentation stopped
             */
            rec->nbins = tmp_nbins;
            /* shift read_bins down so that memory remains contiguous even
             * if nbins has been reduced
             */
            memmove(&rec->write_bins[rec->nbins], rec->read_bins,
                rec->nbins*sizeof(int64_t));
            rec->read_bins = &rec->write_bins[rec->nbins];
        }

        /* now shift the entire record + bins as a contiguous block down in
         * the buffer so that the entire buffer is contiguous
         */
        this_size = sizeof(*rec) + rec->nbins * 2 * sizeof(uint64_t);
        memmove(contig_buf_ptr, rec, this_size);
        contig_buf_ptr += this_size;
        *heatmap_buf_sz += this_size;
    }

    HEATMAP_UNLOCK();

    return;
}

static void heatmap_cleanup()
{
    HEATMAP_LOCK();
    assert(heatmap_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_clear_record_refs(&(heatmap_runtime->rec_id_hash), 1);

    free(heatmap_runtime);
    heatmap_runtime = NULL;

    HEATMAP_UNLOCK();
    return;
}

struct heatmap_runtime* heatmap_runtime_initialize(void)
{
    struct heatmap_runtime* tmp_runtime;
    int ret;
    /* NOTE: this module generates one record per module that uses it, so
     * the memory requirements should be modest
     */
    size_t heatmap_buf_size = sizeof(struct darshan_heatmap_record) + 2*DARSHAN_MAX_HEATMAP_BINS*sizeof(int64_t);
    size_t heatmap_rec_count = DARSHAN_MAX_HEATMAPS;

    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = NULL, /* no reduction; record each rank separately */
#endif
        .mod_output_func = heatmap_output,
        .mod_cleanup_func = heatmap_cleanup
    };

    /* register the heatmap module with darshan core */
    /* note that we aren't holding a lock in this module at this point, but
     * the core will serialize internally and return if this module is
     * already registered */
    ret = darshan_core_register_module(
        DARSHAN_HEATMAP_MOD,
        mod_funcs,
        heatmap_buf_size,
        &heatmap_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
        return(NULL);

    tmp_runtime = malloc(sizeof(*tmp_runtime));
    if(!tmp_runtime)
    {
        darshan_core_unregister_module(DARSHAN_STDIO_MOD);
        return(NULL);
    }
    memset(tmp_runtime, 0, sizeof(*tmp_runtime));

    return(tmp_runtime);
}

darshan_record_id heatmap_register(const char* name)
{
    struct heatmap_record_ref *rec_ref;
    darshan_record_id ret = 0;
    struct heatmap_runtime* tmp_runtime;

    HEATMAP_LOCK();

    if(!heatmap_runtime) {
        /* module not initialized. Drop atomic lock and try to do so */
        HEATMAP_UNLOCK();

        tmp_runtime = heatmap_runtime_initialize();

        HEATMAP_LOCK();
        /* see if someone beat us to it */
        if(heatmap_runtime && tmp_runtime)
            free(tmp_runtime);
        else
            heatmap_runtime = tmp_runtime;
    }

    /* if we exit the above logic without anyone initializing, then we
     * silently return
     */
    if(!heatmap_runtime) {
        HEATMAP_UNLOCK();
        return(0);
    }

    /* generate id for this heatmap */
    ret = darshan_core_gen_record_id(name);

    /* go ahead and instantiate a record now, rather than waiting until the
     * _update() call
     */
    rec_ref = darshan_lookup_record_ref(heatmap_runtime->rec_id_hash, &ret, sizeof(darshan_record_id));
    if(!rec_ref) rec_ref = heatmap_track_new_record(ret, name);

    HEATMAP_UNLOCK();

    return(ret);
}

static void collapse_heatmap(struct darshan_heatmap_record *rec)
{
    int i;

    /* collapse write bins */
    for(i=0; i<DARSHAN_MAX_HEATMAP_BINS; i+=2)
    {
        rec->write_bins[i] += rec->write_bins[i+1]; /* accumulate adjacent bins */
        rec->write_bins[i/2] = rec->write_bins[i];  /* shift down */
    }
    /* zero out second half of heatmap */
    memset(&rec->write_bins[DARSHAN_MAX_HEATMAP_BINS/2], 0, (DARSHAN_MAX_HEATMAP_BINS/2)*sizeof(int64_t));

    /* collapse read bins */
    for(i=0; i<DARSHAN_MAX_HEATMAP_BINS; i+=2)
    {
        rec->read_bins[i] += rec->read_bins[i+1]; /* accumulate adjacent bins */
        rec->read_bins[i/2] = rec->read_bins[i];  /* shift down */
    }
    /* zero out second half of heatmap */
    memset(&rec->read_bins[DARSHAN_MAX_HEATMAP_BINS/2], 0, (DARSHAN_MAX_HEATMAP_BINS/2)*sizeof(int64_t));

    /* double bin width */
    rec->bin_width_seconds *= 2.0;

    return;
}

void heatmap_update(darshan_record_id heatmap_id, int rw_flag,
    int64_t size, double start_time, double end_time)
{
    struct heatmap_record_ref *rec_ref;
    int bin_index = 0;
    double top_boundary, bottom_boundary, seconds_in_bin;

    HEATMAP_PRE_RECORD_VOID();

    rec_ref = darshan_lookup_record_ref(heatmap_runtime->rec_id_hash, &heatmap_id, sizeof(darshan_record_id));
    /* the heatmap should have already been instantiated in the register
     * function; something is wrong if we can't find it now
     */
    if(!rec_ref) { HEATMAP_POST_RECORD(); return; }

    /* is current update out of bounds with histogram size?  if so, collapse */
    while(end_time > rec_ref->heatmap_rec->bin_width_seconds * DARSHAN_MAX_HEATMAP_BINS)
        collapse_heatmap(rec_ref->heatmap_rec);

    /* once we fall through to this point, we know that the current heatmap
     * granularity is sufficiently large to hold this update
     */

    /* loop through bins to be updated (a given access may cross bin
     * boundaries) */
    /* note: counting on the below type conversion to round down to lower
     * integer */
    for(bin_index = start_time/rec_ref->heatmap_rec->bin_width_seconds; bin_index < (int)(end_time/rec_ref->heatmap_rec->bin_width_seconds + 1); bin_index++)
    {
        /* starting assumption about how much time this update spent in
         * current bin
         */
        seconds_in_bin = rec_ref->heatmap_rec->bin_width_seconds;
        /* calculate where bin starts and stops */
        bottom_boundary = bin_index * rec_ref->heatmap_rec->bin_width_seconds;
        top_boundary = bottom_boundary + rec_ref->heatmap_rec->bin_width_seconds;
        /* truncate if update started after bottom boundary */
        if(start_time > bottom_boundary)
            seconds_in_bin -= start_time-bottom_boundary;
        /* truncate if update ended before top boundary */
        if(end_time < top_boundary)
            seconds_in_bin -= top_boundary-end_time;

        if(seconds_in_bin < 0){
            /* this should never happen; really this is an assertion
             * condition but here we just bail out to avoid disrupting the
             * application.
             */
            HEATMAP_POST_RECORD();
            return;
        }

        /* proportionally assign bytes to this bin */
        if(rw_flag == HEATMAP_WRITE)
            rec_ref->heatmap_rec->write_bins[bin_index] +=
                round(size * (seconds_in_bin/(end_time-start_time)));
        else
            rec_ref->heatmap_rec->read_bins[bin_index] +=
                round(size * (seconds_in_bin/(end_time-start_time)));
    }

    HEATMAP_POST_RECORD();

    return;
}

static struct heatmap_record_ref *heatmap_track_new_record(
    darshan_record_id rec_id, const char *name)
{
    struct darshan_heatmap_record *heatmap_rec = NULL;
    struct heatmap_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this record */
    ret = darshan_add_record_ref(&(heatmap_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register with darshan-core so it is persisted in the log file */
    /* include enough space for 2x number of heatmap bins (read and write) */
    heatmap_rec = darshan_core_register_record(
        rec_id,
        name,
        DARSHAN_HEATMAP_MOD,
        sizeof(struct darshan_heatmap_record)+(2*DARSHAN_MAX_HEATMAP_BINS*sizeof(int64_t)),
        NULL);

    if(!heatmap_rec)
    {
        darshan_delete_record_ref(&(heatmap_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    heatmap_rec->base_rec.id = rec_id;
    heatmap_rec->base_rec.rank = my_rank;
    heatmap_rec->bin_width_seconds = DARSHAN_INITIAL_BIN_WIDTH_SECONDS;
    heatmap_rec->nbins = DARSHAN_MAX_HEATMAP_BINS;
    heatmap_rec->write_bins = (int64_t*)((uintptr_t)heatmap_rec + sizeof(*heatmap_rec));
    heatmap_rec->read_bins = (int64_t*)((uintptr_t)heatmap_rec + sizeof(*heatmap_rec) + heatmap_rec->nbins*sizeof(int64_t));
    rec_ref->heatmap_rec = heatmap_rec;
    heatmap_runtime->rec_count++;

    return(rec_ref);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
