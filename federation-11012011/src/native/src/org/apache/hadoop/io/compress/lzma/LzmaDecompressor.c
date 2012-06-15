/*
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

#if defined HAVE_CONFIG_H
  #include <config.h>
#endif

#if defined HAVE_STDIO_H
  #include <stdio.h>
#else
  #error 'stdio.h not found'
#endif

#if defined HAVE_STDLIB_H
  #include <stdlib.h>
#else
  #error 'stdlib.h not found'
#endif

#if defined HAVE_STRING_H
  #include <string.h>
#else
  #error 'string.h not found'
#endif

#if defined HAVE_DLFCN_H
  #include <dlfcn.h>
#else
  #error 'dlfcn.h not found'
#endif

#include "org_apache_hadoop_io_compress_lzma.h"

// The lzma library-handle
static void *liblzma = NULL;

static jfieldID LzmaDecompressor_clazz;
static jfieldID LzmaDecompressor_stream;
static jfieldID LzmaDecompressor_compressedDirectBuf;
static jfieldID LzmaDecompressor_compressedDirectBufOff;
static jfieldID LzmaDecompressor_compressedDirectBufLen;
static jfieldID LzmaDecompressor_uncompressedDirectBuf;
static jfieldID LzmaDecompressor_directBufferSize;
static jfieldID LzmaDecompressor_finished;

static int (*dlsym_lzma_easy_encoder)(lzma_stream *strm, uint32_t level, lzma_check check);
static int (*dlsym_lzma_code)(lzma_stream *strm, lzma_action action);
static int (*dlsym_lzma_end)(lzma_stream *strm);
static int (*dlsym_lzma_auto_decoder)(lzma_stream *strm, uint64_t memlimit, uint32_t flags);

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_lzma_LzmaDecompressor_initIDs(
	JNIEnv *env, jclass class
	) {
	// Load liblzma.so
  liblzma = dlopen(HADOOP_LZMA_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
	if (!liblzma) {
	  THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load liblzma.so");
	  return;
	}

	// Locate the requisite symbols from liblzma.so
	dlerror();                                 // Clear any existing error
	LOAD_DYNAMIC_SYMBOL(dlsym_lzma_easy_encoder, env, liblzma, "lzma_easy_encoder");
	LOAD_DYNAMIC_SYMBOL(dlsym_lzma_code, env, liblzma, "lzma_code");
	LOAD_DYNAMIC_SYMBOL(dlsym_lzma_end, env, liblzma, "lzma_end");
	LOAD_DYNAMIC_SYMBOL(dlsym_lzma_auto_decoder, env, liblzma, "lzma_auto_decoder");

	// Initialize the requisite fieldIds
    LzmaDecompressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz",
                                                      "Ljava/lang/Class;");
    LzmaDecompressor_stream = (*env)->GetFieldID(env, class, "stream", "J");

    LzmaDecompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");

    LzmaDecompressor_compressedDirectBuf = (*env)->GetFieldID(env, class,
    											"compressedDirectBuf",
    											"Ljava/nio/Buffer;");
    LzmaDecompressor_compressedDirectBufOff = (*env)->GetFieldID(env, class,
    										"compressedDirectBufOff", "I");
    LzmaDecompressor_compressedDirectBufLen = (*env)->GetFieldID(env, class,
    										"compressedDirectBufLen", "I");
    LzmaDecompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class,
    											"uncompressedDirectBuf",
    											"Ljava/nio/Buffer;");
    LzmaDecompressor_directBufferSize = (*env)->GetFieldID(env, class,
    											"directBufferSize", "I");
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_lzma_LzmaDecompressor_init(
	JNIEnv *env, jclass class
	) {
	//Create a lzma_stream
	lzma_stream *stream = NULL;

	stream = malloc(sizeof(lzma_stream));
	if (!stream) {
		THROW(env, "java/lang/OutOfMemoryError", NULL);
		return (jlong)0;
	}

	// Initialize stream
	lzma_stream tmp = LZMA_STREAM_INIT;
	*stream = tmp;

      lzma_ret ret = (*dlsym_lzma_auto_decoder)(stream, 100<<20, 0);

    if (ret != LZMA_OK) {
	    // Contingency - Report error by throwing appropriate exceptions
	    free(stream);
	    stream = NULL;
	    THROW(env, "java/lang/InternalError", NULL);
    }
	return (jlong)(stream);
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_io_compress_lzma_LzmaDecompressor_decompressBytesDirect(
	JNIEnv *env, jobject this
	) {
	// Get members of LzmaCompressor
    lzma_stream *stream = (lzma_stream*)(
    						(*env)->GetLongField(env, this,
    									LzmaDecompressor_stream)
    					);
    if (!stream) {
		THROW(env, "java/lang/NullPointerException", NULL);
		return (jint)0;
    }

    // Get members of ZlibDecompressor
    jobject clazz = (*env)->GetStaticObjectField(env, this,
                                                 LzmaDecompressor_clazz);
	jarray compressed_direct_buf = (jarray)(*env)->GetObjectField(env, this,
											LzmaDecompressor_compressedDirectBuf);
	jint compressed_direct_buf_off = (*env)->GetIntField(env, this,
									LzmaDecompressor_compressedDirectBufOff);
	jint compressed_direct_buf_len = (*env)->GetIntField(env, this,
									LzmaDecompressor_compressedDirectBufLen);

	jarray uncompressed_direct_buf = (jarray)(*env)->GetObjectField(env, this,
											LzmaDecompressor_uncompressedDirectBuf);
	jint uncompressed_direct_buf_len = (*env)->GetIntField(env, this,
										LzmaDecompressor_directBufferSize);

    // Get the input direct buffer
    LOCK_CLASS(env, clazz, "LzmaDecompressor");
	uint8_t *compressed_bytes = (*env)->GetDirectBufferAddress(env,
										compressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "LzmaDecompressor");

	if (!compressed_bytes) {
	    return (jint)0;
	}

    // Get the output direct buffer
    LOCK_CLASS(env, clazz, "LzmaDecompressor");
	uint8_t *uncompressed_bytes = (*env)->GetDirectBufferAddress(env,
											uncompressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "LzmaDecompressor");

	if (!uncompressed_bytes) {
	    return (jint)0;
	}

	// Re-calibrate the lzma_stream
	stream->next_in  = compressed_bytes + compressed_direct_buf_off;
	stream->next_out = uncompressed_bytes;
	stream->avail_in  = compressed_direct_buf_len;
	stream->avail_out = uncompressed_direct_buf_len;

	// Decompress
	lzma_ret ret = dlsym_lzma_code(stream, LZMA_RUN);

	jint no_decompressed_bytes = 0;

	// Contingency? - Report error by throwing appropriate exceptions
	switch (ret) {
		case LZMA_STREAM_END:
		{
		    (*env)->SetBooleanField(env, this, LzmaDecompressor_finished, JNI_TRUE);
		} // cascade down
		case LZMA_OK:
		{
		    compressed_direct_buf_off += compressed_direct_buf_len - stream->avail_in;
		    (*env)->SetIntField(env, this, LzmaDecompressor_compressedDirectBufOff,
		    			compressed_direct_buf_off);
		    (*env)->SetIntField(env, this, LzmaDecompressor_compressedDirectBufLen,
		    			stream->avail_in);
		    no_decompressed_bytes = uncompressed_direct_buf_len - stream->avail_out;
		}
		break;
		default:
		{
        fprintf(stderr, "java/lang/InternalError would throw: %d\n", (int)ret);
		    THROW(env, "java/lang/InternalError", NULL);
		}
		break;
    }
    return no_decompressed_bytes;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_lzma_LzmaDecompressor_getBytesRead(
	JNIEnv *env, jclass class, jlong stream
	) {
     return ((lzma_stream*)(stream))->total_in;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_lzma_LzmaDecompressor_getBytesWritten(
	JNIEnv *env, jclass class, jlong stream
	) {
    return ((lzma_stream*)(stream))->total_out;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_lzma_LzmaDecompressor_end(
	JNIEnv *env, jclass class, jlong stream
	) {
       dlsym_lzma_end((lzma_stream*)(stream));
	free((lzma_stream*)(stream));
}

/**
 * vim: sw=2: ts=2: et:
 */

