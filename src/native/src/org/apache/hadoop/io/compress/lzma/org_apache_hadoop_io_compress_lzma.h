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

#if !defined ORG_APACHE_HADOOP_IO_COMPRESS_LZMA_LZMA_H
#define ORG_APACHE_HADOOP_IO_COMPRESS_LZMA_LZMA_H

#if defined HAVE_CONFIG_H
  #include <config.h>
#endif

#if defined HAVE_STDDEF_H
  #include <stddef.h>
#else
  #error 'stddef.h not found'
#endif

#if defined HAVE_DLFCN_H
  #include <dlfcn.h>
#else
  #error "dlfcn.h not found"
#endif

#if defined HAVE_JNI_H
  #include <jni.h>
#else
  #error 'jni.h not found'
#endif

//#if defined HAVE_LZMA_H
  #include <lzma/lzma.h>
//#else
//  #error 'lzma.h not found'
//#endif

#include "org_apache_hadoop.h"

/*
 * No compression; the data is just wrapped into .lzma
 * container.
 */
#define	LZMA_EASY_COPY 0
#define	LZMA_EASY_LZMA2_1 1
#define	LZMA_EASY_LZMA_2   2
#define	LZMA_EASY_LZMA_3   3
#define	LZMA_EASY_LZMA_4   4
#define	LZMA_EASY_LZMA_5   5
#define	LZMA_EASY_LZMA_6   6
#define	LZMA_EASY_LZMA_7   7
#define	LZMA_EASY_LZMA_8   8
#define	LZMA_EASY_LZMA_9   9

#define 	COMPRESS_LEVEL_BEST LZMA_EASY_LZMA_9
#define COMPRESS_LEVEL_DEFAULT LZMA_EASY_LZMA_7

#define kBufferSize (1 << 15)

/* A helper macro to convert the java 'function-pointer' to a void*. */
#define FUNC_PTR(func_ptr) ((void*)((ptrdiff_t)(func_ptr)))

/* A helper macro to convert the void* to the java 'function-pointer'. */
#define JLONG(func_ptr) ((jlong)((ptrdiff_t)(func_ptr)))

#endif //ORG_APACHE_HADOOP_IO_COMPRESS_LZMA_LZMA_H
