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
#define MODULE_ID_LIMIT 14

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

    ret = darshan_accumulator_create(ctx->mod_id, &acc);
    munit_assert_int(ret, ==, 0);

    ret = darshan_accumulator_destroy(acc);
    munit_assert_int(ret, ==, 0);

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
