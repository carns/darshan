/*
 * Copyright (C) 2022 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

/* This function implements the accumulator API (darshan_accumlator*)
 * functions in darshan-logutils.h.
 */

#include "darshan-logutils.h"

struct darshan_accumulator_st {

};

int darshan_accumulator_create(darshan_module_id id,
                               darshan_accumulator*   new_accumulator)
{
    return(-1);
}

int darshan_accumulator_inject(darshan_accumulator accumulator,
                               void*               record_array,
                               int                 record_count)
{
    return(-1);
}

int darshan_accumulator_emit(darshan_accumulator             accumulator,
                             struct darshan_derived_metrics* metrics,
                             void**                          summation_record)
{
    return(-1);
}

int darshan_accumulator_destroy(darshan_accumulator accumulator)
{
    return(-1);
}
