#include "hdfs.h"
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIBHDFS_HDFSHIPERF_H
#define LIBHDFS_HDFSHIPERF_H
/* Low-level additions to the libhdfs API that allow optimizations that can
 * reduce the number of JNI array allocations and copies */


#endif /*LIBHDFS_HDFSHIPERF_H*/

#ifdef __cplusplus
extern  "C" {
#endif

/* A buffer type that can be reused for multiple hdfs reads */
typedef struct hdfsBuffer* hdfsBuf;

/**
 * hdfsCreateBuffer
 * Creates a Java byte array to be reused as a buffer for hdfs reads.
 * Returns NULL on error
 * @param size The size in bytes of the buffer to create
 * @param jni_cache_size cache reads of less than this size to avoid extra JNI
 *      calls
 */
hdfsBuf hdfsCreateBuffer(tSize size, tSize jni_cache_size);

/**
 * hdfsDestroyBuffer
 * Destroys a previously created buffer
 * returns 0 on success'
 * @param buf A hdfsBuf previously created with hdfsCreateBuffer and not yet
 *      destroyed
 */
tSize hdfsDestroyBuffer(hdfsBuf buf);

/**
 * hdfsBufSize
 * Returns the size in bytes of a previously created buffer
 * Returns -1 if buf invalid
 * @param buf A hdfsbuf
 */
tSize hdfsBufSize(hdfsBuf buf);

/**
 * hdfsBufRead
 * Read from hdfsBuf into a character array
 * @param src The hdfsBuf to read from
 * @param srcoff The offset within the hdfsBuf to read from
 * @param dst A character array to read into
 * @param len The length of data to read (must be <= size of dst
 *      and <= hdfsBufSize(src) - srcoff)
 * returns the number of bytes read, or -1 on error
 */
tSize hdfsBufRead(hdfsBuf src, tSize srcoff, void *dst, tSize len);

/**
 * hdfsRead_direct
 * Read from a file into a hdfsBuf
 * @param fs a distributed file system
 * @param f an open file handle
 * @param buffer: the buffer to read data into
 * @param off The position in the buffer to read into.
 * @param length the length of data to read (must be less than
 *                                            hdfsBufSize(buffer))
 */
tSize hdfsRead_direct(hdfsFS fs, hdfsFile f, hdfsBuf buffer,
                                    tSize off, tSize length);
/**
 * hdfsPread_direct
 * Read from a file into a hdfsBuf
 * @param fs a distributed file system
 * @param f an open file handle
 * @param position the position within the file to read from
 * @param buffer the buffer to read data into
 * @param off The position in the buffer to read into.
 * @param length the length of data to read (must be less than
 *                                            hdfsBufSize(buffer))
 */
tSize hdfsPread_direct(hdfsFS fs, hdfsFile f, tOffset position,
                hdfsBuf buffer, tSize off, tSize length);

#ifdef __cplusplus
}
#endif

/**
 * vim: ts=4: sw=4: et
 */
