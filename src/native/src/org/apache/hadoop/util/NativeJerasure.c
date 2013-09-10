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
#include <arpa/inet.h>
#include <time.h>
#include <assert.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "galois.h"
#include "jerasure.h"
#include "reed_sol.h"
#include "NativeJerasure.h"
#include "org_apache_hadoop.h"
#include "org_apache_hadoop_util_NativeJerasure.h"

JNIEXPORT void JNICALL Java_org_apache_hadoop_util_NativeJerasure_nativeInit
  (JNIEnv *env, jclass clazz, jint stripeSize, jint paritySize) {
  matrix = reed_sol_vandermonde_coding_matrix(stripeSize, paritySize, WORD_SIZE);
}

void printArray(char** output, int num, int dataLength) {
  int i;
  int j;
  for (i = 0; i < num; i++) {
    printf("Row %d:", i);
    for (j = 0; j < dataLength; j++) {
      printf(" %d", output[i][j]);
    }
    printf("\n");
  }
}

void printTime(struct timespec* stime, const char* message) {
  struct timespec etime;
  clock_gettime(CLOCK_REALTIME, &etime);
  printf( "%s:%lfms\n", message, (etime.tv_nsec - stime->tv_nsec)/1e6);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_util_NativeJerasure_nativeEncodeBulk
  (JNIEnv *env, jclass clazz, jobjectArray inputBuffers, jobjectArray outputBuffers,
  jint stripeSize, jint paritySize, jint dataLength) {

  char* data[stripeSize];
  char* coding[paritySize];
  int i;

  for (i = 0; i < stripeSize; i++) {
    jobject j_inputBuffer = (*env)->GetObjectArrayElement(env, inputBuffers, i);
    data[i] = (char*)(*env)->GetDirectBufferAddress(env, j_inputBuffer);
  }

  for (i = 0; i < paritySize; i++) {
    jobject j_outputBuffer = (*env)->GetObjectArrayElement(env, outputBuffers, i);
    coding[i] = (char*)(*env)->GetDirectBufferAddress(env, j_outputBuffer);
    memset(coding[i], 0, dataLength);
  }

	jerasure_matrix_encode(stripeSize, paritySize, WORD_SIZE, matrix, data, coding, dataLength);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_util_NativeJerasure_nativeDecodeBulk
  (JNIEnv *env, jclass clazz, jobjectArray inputBuffers, jobjectArray outputBuffers,
  jintArray erasedLocationsArray, jint dataLength, jint stripeSize,
  jint paritySize, jint numErasedLocations) {
	int* erasures = (int *)malloc(sizeof(int)*(stripeSize + paritySize));
  char* data[stripeSize];
  char* coding[paritySize];
  int i;
  char* outputBufs[numErasedLocations];

  for (i = 0; i < stripeSize + paritySize; i++) {
    jobject j_inputBuffer = (*env)->GetObjectArrayElement(env, inputBuffers, i);
    if (i < paritySize) {
      coding[i] = (char*)(*env)->GetDirectBufferAddress(env, j_inputBuffer);
    } else {
      data[i - paritySize] = (char*)(*env)->GetDirectBufferAddress(env, j_inputBuffer);
    }
  }

  int index = 0;
  jint* erasedLocations = (*env)->GetIntArrayElements(env, erasedLocationsArray, 0);
  for (i = 0;i < numErasedLocations;i++) {
    jobject j_outputBuffer = (*env)->GetObjectArrayElement(env, outputBuffers, i);
    outputBufs[i] = (char*)(*env)->GetDirectBufferAddress(env, j_outputBuffer);
    memset(outputBufs[i], 0, dataLength);
    erasures[index] = erasedLocations[i];
    index += 1;
  }
  erasures[index] = -1;

  for (i = 0;i < index;i++) {
    if (erasures[i] < paritySize) {
      erasures[i] += stripeSize;
    } else {
      erasures[i] -= paritySize;
    }
  }

  if (matrix == NULL) {
    printf("Error setting matrix");
  } else {
    jerasure_matrix_decode(
      stripeSize, paritySize, WORD_SIZE, matrix, 1, erasures, data, coding, dataLength);
  }

  for (i = 0;i < numErasedLocations;i++) {
    int eraseIndex = erasedLocations[i];
    if (eraseIndex < paritySize) {
      memcpy(outputBufs[i], coding[eraseIndex], dataLength);
    } else {
      memcpy(outputBufs[i], data[eraseIndex - paritySize], dataLength);
    }
  }
}
