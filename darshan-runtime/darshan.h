/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_H
#define __DARSHAN_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <mpi.h>

#include "darshan-log-format.h"

/* Environment variable to override CP_JOBID */
#define CP_JOBID_OVERRIDE "DARSHAN_JOBID"

/* Environment variable to override __CP_LOG_PATH */
#define CP_LOG_PATH_OVERRIDE "DARSHAN_LOGPATH"

/* Environment variable to override __CP_LOG_PATH */
#define CP_LOG_HINTS_OVERRIDE "DARSHAN_LOGHINTS"

/* Environment variable to override __CP_MEM_ALIGNMENT */
#define CP_MEM_ALIGNMENT_OVERRIDE "DARSHAN_MEMALIGN"

struct darshan_module_funcs
{
    /* disable futher instrumentation within a module */
    void (*disable_instrumentation)(void);
    /* TODO: */
    void (*prepare_for_reduction)(
        darshan_record_id *shared_recs,
        int *shared_rec_count, /* in/out shared record count */
        void **send_buf,
        void **recv_buf,
        int *rec_size
    );
    /* TODO: */
    void (*reduce_record)(
        void* infile_v,
        void* inoutfile_v,
        int *len,
        MPI_Datatype *datatype
    );
    /* retrieve module data to write to log file */
    void (*get_output_data)(
        void** buf, /* output parameter to save module buffer address */
        int* size /* output parameter to save module buffer size */
    );
    /* shutdown module data structures */
    void (*shutdown)(void);
};

/*****************************************************
* darshan-core functions exported to darshan modules *
*****************************************************/

void darshan_core_register_module(
    darshan_module_id mod_id,
    struct darshan_module_funcs *funcs,
    int *runtime_mem_limit);

void darshan_core_lookup_record_id(
    void *name,
    int len,
    int printable_flag,
    darshan_module_id mod_id,
    darshan_record_id *id);

double darshan_core_wtime(void);

/***********************************************
* darshan-common functions for darshan modules *
***********************************************/

char* darshan_clean_file_path(const char* path);

#endif /* __DARSHAN_H */
