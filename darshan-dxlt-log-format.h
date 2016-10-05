/*
 * Copyright (C) 2016 Intel Corporation.
 * See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_DXLT_LOG_FORMAT_H
#define __DARSHAN_DXLT_LOG_FORMAT_H

/* current DXLT log format version */
#define DXLT_POSIX_VER 1
#define DXLT_MPIIO_VER 1

/*
 * DXLT, the segment_info structure maintains detailed Segment IO tracing
 * information
 */
typedef struct segment_info {
    int64_t offset;
    int64_t length;
    double start_time;
    double end_time;
} segment_info;

#define X(a) a,
#undef X

/* file record structure for DXLT files. a record is created and stored for
 * every DXLT file opened by the original application. For the DXLT module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct dxlt_file_record {
    struct darshan_base_record base_rec;
    int64_t shared_record; /* -1 means it is a shared file record */

    int32_t stripe_size;
    int32_t stripe_count;
    OST_ID *ost_ids;

    int64_t write_count;
    int64_t write_available_buf;
    segment_info *write_traces;

    int64_t read_count;
    int64_t read_available_buf;
    segment_info *read_traces;
};

#endif /* __DARSHAN_DXLT_LOG_FORMAT_H */
