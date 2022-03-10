/*
 * Copyright (C) 2022 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdio.h>
#include "munit/munit.h"

#include <darshan-logutils.h>

/* TODO: replace this with DARSHAN_KNOWN_MODULE_COUNT when PR #668 lands.
 * This test needs a value to indicate how many entries are populated in
 * darshan_module_names, independent of the maximum module id value.
 */
#define MODULE_ID_LIMIT 15

static void posix_set_dummy_record(void* buffer);
static void posix_validate_double_dummy_record(void* buffer, int shared_file_flag);

void (*set_dummy_fn[MODULE_ID_LIMIT])(void*) = {
    NULL, /* DARSHAN_NULL_MOD */
    posix_set_dummy_record, /* DARSHAN_POSIX_MOD */
    NULL, /* DARSHAN_MPIIO_MOD */
    NULL, /* DARSHAN_H5F_MOD */
    NULL, /* DARSHAN_H5D_MOD */
    NULL, /* DARSHAN_PNETCDF_MOD */
    NULL, /* DARSHAN_BGQ_MOD */
    NULL, /* DARSHAN_LUSTRE_MOD */
    NULL, /* DARSHAN_STDIO_MOD */
    NULL, /* DXT_POSIX_MOD */
    NULL, /* DXT_MPIIO_MOD */
    NULL, /* DARSHAN_MDHIM_MOD */
    NULL, /* DARSHAN_APXC_MOD */
    NULL, /* DARSHAN_APMPI_MOD */
    NULL /* DARSHAN_HEATMAP_MOD */
};

void (*validate_double_dummy_fn[MODULE_ID_LIMIT])(void*, int) = {
    NULL, /* DARSHAN_NULL_MOD */
    posix_validate_double_dummy_record, /* DARSHAN_POSIX_MOD */
    NULL, /* DARSHAN_MPIIO_MOD */
    NULL, /* DARSHAN_H5F_MOD */
    NULL, /* DARSHAN_H5D_MOD */
    NULL, /* DARSHAN_PNETCDF_MOD */
    NULL, /* DARSHAN_BGQ_MOD */
    NULL, /* DARSHAN_LUSTRE_MOD */
    NULL, /* DARSHAN_STDIO_MOD */
    NULL, /* DXT_POSIX_MOD */
    NULL, /* DXT_MPIIO_MOD */
    NULL, /* DARSHAN_MDHIM_MOD */
    NULL, /* DARSHAN_APXC_MOD */
    NULL, /* DARSHAN_APMPI_MOD */
    NULL /* DARSHAN_HEATMAP_MOD */
};


struct test_context {
    darshan_module_id mod_id;
    struct darshan_mod_logutil_funcs* mod_fns;
};

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) user_data;
    int i;
    int found = 0;

    const char* module_name = munit_parameters_get(params, "module_name");

    struct test_context* ctx = calloc(1, sizeof(*ctx));
    munit_assert_not_null(ctx);

    /* Look for module with this name and keep a reference to it's logutils
     * functions.
     */
    for(i=0; i<MODULE_ID_LIMIT; i++) {
        if (strcmp(darshan_module_names[i], module_name) == 0) {
            ctx->mod_id = i;
            ctx->mod_fns = mod_logutils[i];
            found=1;
            break;
        }
    }
    munit_assert_int(found, ==, 1);

    return ctx;
}

static void test_context_tear_down(void *data)
{
    struct test_context *ctx = (struct test_context*)data;

    free(ctx);
}

/* test repeated init/finalize cycles, server mode */
static MunitResult inject_shared_file_records(const MunitParameter params[], void* data)
{
    struct test_context* ctx = (struct test_context*)data;
    int ret;
    darshan_accumulator acc;
    struct darshan_derived_metrics metrics;
    void* record1;
    void* record2;
    void* record_agg;
    struct darshan_base_record* base_rec;

    record1 = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record1);
    record2 = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record2);
    record_agg = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record_agg);

    /* make sure we have a function defined to set example records */
    munit_assert_not_null(set_dummy_fn[ctx->mod_id]);

    /* create example records, shared file but different ranks */
    set_dummy_fn[ctx->mod_id](record1);
    set_dummy_fn[ctx->mod_id](record2);
    base_rec = record2;
    base_rec->rank++;

    /**** shared file aggregation ****/

    ret = darshan_accumulator_create(ctx->mod_id, &acc);
    munit_assert_int(ret, ==, 0);

    /* inject two example records */
    ret = darshan_accumulator_inject(acc, record1, 1);
    munit_assert_int(ret, ==, 0);
    ret = darshan_accumulator_inject(acc, record1, 1);
    munit_assert_int(ret, ==, 0);

    /* emit results */
    ret = darshan_accumulator_emit(acc, &metrics, record_agg);
    munit_assert_int(ret, ==, 0);

    /* sanity check */
    validate_double_dummy_fn[ctx->mod_id](record_agg, 1);

    ret = darshan_accumulator_destroy(acc);
    munit_assert_int(ret, ==, 0);

    /**** unique file aggregation ****/

    /* change id hash in one record and repeat test case */
    base_rec->id++;

    ret = darshan_accumulator_create(ctx->mod_id, &acc);
    munit_assert_int(ret, ==, 0);

    /* inject two example records */
    ret = darshan_accumulator_inject(acc, record1, 1);
    munit_assert_int(ret, ==, 0);
    ret = darshan_accumulator_inject(acc, record1, 1);
    munit_assert_int(ret, ==, 0);

    /* emit results */
    ret = darshan_accumulator_emit(acc, &metrics, record_agg);
    munit_assert_int(ret, ==, 0);

    /* sanity check */
    validate_double_dummy_fn[ctx->mod_id](record_agg, 0);

    ret = darshan_accumulator_destroy(acc);
    munit_assert_int(ret, ==, 0);

    free(record1);
    free(record2);
    free(record_agg);

    return MUNIT_OK;
}

static char* module_name_params[] = {"POSIX", "STDIO", "MPI-IO", NULL};

static MunitParameterEnum test_params[]
    = {{"module_name", module_name_params}, {NULL, NULL}};

static MunitTest tests[] = {
    { "/inject-shared-file-records", inject_shared_file_records, test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params},
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    "/darshan-accumulator", tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};


int main(int argc, char **argv)
{
    return munit_suite_main(&test_suite, NULL, argc, argv);
}

/* Set example values for record of type posix.  As elsewhere in the
 * logutils API, the size of the buffer is implied.
 */
static void posix_set_dummy_record(void* buffer) {
    struct darshan_posix_file* pfile = buffer;

    /* This function must be updated (or at least checked) if the posix
     * module log format changes
     */
    munit_assert_int(DARSHAN_POSIX_VER, ==, 4);

    pfile->base_rec.id = 15574190512568163195UL;
    pfile->base_rec.rank = 0;

    pfile->counters[POSIX_OPENS] = 16;
    pfile->counters[POSIX_FILENOS] = 0;
    pfile->counters[POSIX_DUPS] = 0;
    pfile->counters[POSIX_READS] = 4;
    pfile->counters[POSIX_WRITES] = 4;
    pfile->counters[POSIX_SEEKS] = 0;
    pfile->counters[POSIX_STATS] = 0;
    pfile->counters[POSIX_MMAPS] = -1;
    pfile->counters[POSIX_FSYNCS] = 0;
    pfile->counters[POSIX_FDSYNCS] = 0;
    pfile->counters[POSIX_RENAME_SOURCES] = 0;
    pfile->counters[POSIX_RENAME_TARGETS] = 0;
    pfile->counters[POSIX_RENAMED_FROM] = 0;
    pfile->counters[POSIX_MODE] = 436;
    pfile->counters[POSIX_BYTES_READ] = 67108864;
    pfile->counters[POSIX_BYTES_WRITTEN] = 67108864;
    pfile->counters[POSIX_MAX_BYTE_READ] = 67108863;
    pfile->counters[POSIX_MAX_BYTE_WRITTEN] = 67108863;
    pfile->counters[POSIX_CONSEC_READS] = 0;
    pfile->counters[POSIX_CONSEC_WRITES] = 0;
    pfile->counters[POSIX_SEQ_READS] = 3;
    pfile->counters[POSIX_SEQ_WRITES] = 3;
    pfile->counters[POSIX_RW_SWITCHES] = 4;
    pfile->counters[POSIX_MEM_NOT_ALIGNED] = 0;
    pfile->counters[POSIX_MEM_ALIGNMENT] = 8;
    pfile->counters[POSIX_FILE_NOT_ALIGNED] = 0;
    pfile->counters[POSIX_FILE_ALIGNMENT] = 4096;
    pfile->counters[POSIX_MAX_READ_TIME_SIZE] = 16777216;
    pfile->counters[POSIX_MAX_WRITE_TIME_SIZE] = 16777216;
    pfile->counters[POSIX_SIZE_READ_0_100] = 0;
    pfile->counters[POSIX_SIZE_READ_100_1K] = 0;
    pfile->counters[POSIX_SIZE_READ_1K_10K] = 0;
    pfile->counters[POSIX_SIZE_READ_10K_100K] = 0;
    pfile->counters[POSIX_SIZE_READ_100K_1M] = 0;
    pfile->counters[POSIX_SIZE_READ_1M_4M] = 0;
    pfile->counters[POSIX_SIZE_READ_4M_10M] = 0;
    pfile->counters[POSIX_SIZE_READ_10M_100M] = 4;
    pfile->counters[POSIX_SIZE_READ_100M_1G] = 0;
    pfile->counters[POSIX_SIZE_READ_1G_PLUS] = 0;
    pfile->counters[POSIX_SIZE_WRITE_0_100] = 0;
    pfile->counters[POSIX_SIZE_WRITE_100_1K] = 0;
    pfile->counters[POSIX_SIZE_WRITE_1K_10K] = 0;
    pfile->counters[POSIX_SIZE_WRITE_10K_100K] = 0;
    pfile->counters[POSIX_SIZE_WRITE_100K_1M] = 0;
    pfile->counters[POSIX_SIZE_WRITE_1M_4M] = 0;
    pfile->counters[POSIX_SIZE_WRITE_4M_10M] = 0;
    pfile->counters[POSIX_SIZE_WRITE_10M_100M] = 4;
    pfile->counters[POSIX_SIZE_WRITE_100M_1G] = 0;
    pfile->counters[POSIX_SIZE_WRITE_1G_PLUS] = 0;
    pfile->counters[POSIX_STRIDE1_STRIDE] = 0;
    pfile->counters[POSIX_STRIDE2_STRIDE] = 0;
    pfile->counters[POSIX_STRIDE3_STRIDE] = 0;
    pfile->counters[POSIX_STRIDE4_STRIDE] = 0;
    pfile->counters[POSIX_STRIDE1_COUNT] = 0;
    pfile->counters[POSIX_STRIDE2_COUNT] = 0;
    pfile->counters[POSIX_STRIDE3_COUNT] = 0;
    pfile->counters[POSIX_STRIDE4_COUNT] = 0;
    pfile->counters[POSIX_ACCESS1_ACCESS] = 16777216;
    pfile->counters[POSIX_ACCESS2_ACCESS] = 0;
    pfile->counters[POSIX_ACCESS3_ACCESS] = 0;
    pfile->counters[POSIX_ACCESS4_ACCESS] = 0;
    pfile->counters[POSIX_ACCESS1_COUNT] = 8;
    pfile->counters[POSIX_ACCESS2_COUNT] = 0;
    pfile->counters[POSIX_ACCESS3_COUNT] = 0;
    pfile->counters[POSIX_ACCESS4_COUNT] = 0;
    pfile->counters[POSIX_FASTEST_RANK] = 2;
    pfile->counters[POSIX_FASTEST_RANK_BYTES] = 33554432;
    pfile->counters[POSIX_SLOWEST_RANK] = 3;
    pfile->counters[POSIX_SLOWEST_RANK_BYTES] = 33554432;

    pfile->fcounters[POSIX_F_OPEN_START_TIMESTAMP] = 0.008787;
    pfile->fcounters[POSIX_F_READ_START_TIMESTAMP] = 0.079433;
    pfile->fcounters[POSIX_F_WRITE_START_TIMESTAMP] = 0.009389;
    pfile->fcounters[POSIX_F_CLOSE_START_TIMESTAMP] = 0.008901;
    pfile->fcounters[POSIX_F_OPEN_END_TIMESTAMP] = 0.079599;
    pfile->fcounters[POSIX_F_READ_END_TIMESTAMP] = 0.088423;
    pfile->fcounters[POSIX_F_WRITE_END_TIMESTAMP] = 0.042157;
    pfile->fcounters[POSIX_F_CLOSE_END_TIMESTAMP] = 0.088617;
    pfile->fcounters[POSIX_F_READ_TIME] = 0.030387;
    pfile->fcounters[POSIX_F_WRITE_TIME] = 0.082557;
    pfile->fcounters[POSIX_F_META_TIME] = 0.000177;
    pfile->fcounters[POSIX_F_MAX_READ_TIME] = 0.008990;
    pfile->fcounters[POSIX_F_MAX_WRITE_TIME] = 0.032618;
    pfile->fcounters[POSIX_F_FASTEST_RANK_TIME] = 0.015122;
    pfile->fcounters[POSIX_F_SLOWEST_RANK_TIME] = 0.040990;
    pfile->fcounters[POSIX_F_VARIANCE_RANK_TIME] = 0.000090;
    pfile->fcounters[POSIX_F_VARIANCE_RANK_BYTES] = 0.000000;

    return;
}

/* Validate that the aggregation produced sane values after being used to
 * aggregate 2 rank records for the same file.
 */
static void posix_validate_double_dummy_record(void* buffer, int shared_file_flag) {
    struct darshan_posix_file* pfile = buffer;

    /* This function must be updated (or at least checked) if the posix
     * module log format changes
     */
    munit_assert_int(DARSHAN_POSIX_VER, ==, 4);

    /* check base record */
    if(shared_file_flag)
        munit_assert_int64(pfile->base_rec.id, ==, 15574190512568163195UL);
    else
        munit_assert_int64(pfile->base_rec.id, ==, 0);
    munit_assert_int64(pfile->base_rec.rank, ==, -1);

    /* double */
    munit_assert_int64(pfile->counters[POSIX_OPENS], ==, 32);
    /* stay set at -1 */
    munit_assert_int64(pfile->counters[POSIX_MMAPS], ==, -1);
    /* stay set */
    munit_assert_int64(pfile->counters[POSIX_MODE], ==, 436);

    /* "fastest" behavior should change depending on if records are shared
     * or not
     */
    if(shared_file_flag)
        munit_assert_int64(pfile->counters[POSIX_FASTEST_RANK], ==, 2);
    else
        munit_assert_int64(pfile->counters[POSIX_FASTEST_RANK], ==, -1);

    /* double */
    munit_assert_int64(pfile->fcounters[POSIX_F_READ_TIME], ==, .060774);

    /* variance should be cleared right now */
    munit_assert_int64(pfile->fcounters[POSIX_F_VARIANCE_RANK_TIME], ==, 0);

    return;
}
