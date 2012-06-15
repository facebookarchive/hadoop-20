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

#include "org_apache_hadoop_syscall_LinuxSystemCall.h"
#include <sys/types.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>

static int getFd(JNIEnv *env, jobject impl)
{
  JNIEnv e = *env;
  jclass clazz;
  jfieldID fid;
  jobject fdesc;

  /* get the FileDescriptor from the SocketImpl */
  if (!(clazz = e->GetObjectClass(env,impl)) ||
      !(fid = e->GetFieldID(env,clazz,"fd","Ljava/io/FileDescriptor;")) ||
      !(fdesc = e->GetObjectField(env,impl,fid))) return -1;

  /* get the fd from the FileDescriptor */
  if (!(clazz = e->GetObjectClass(env,fdesc)) ||
      !(fid = e->GetFieldID(env,clazz,"fd","I"))) return -1;

  /* return the descriptor */
  return e->GetIntField(env,fdesc,fid);
}

static int getFdBySocket(JNIEnv *env, jobject sock)
{
  JNIEnv e = *env;
  jclass clazz;
  jfieldID fid;
  jobject impl;

  /* get the SocketImpl from the Socket */
  if (!(clazz = e->GetObjectClass(env,sock)) ||
      !(fid = e->GetFieldID(env,clazz,"impl","Ljava/net/SocketImpl;")) ||
      !(impl = e->GetObjectField(env,sock,fid)))  return -1;

  return getFd(env, impl);

}

JNIEXPORT jint JNICALL
  Java_org_apache_hadoop_syscall_LinuxSystemCall_setSockOpt
(JNIEnv *env, jobject obj, jobject sock, jint level, jint opt, jint val)
{
  int fd, sval;
  fd = getFd(env, sock);
  sval = val;

  int ret = setsockopt(fd, level, opt, (void *)&sval, sizeof(int));

  if (ret < 0) {
    printf("setsockopt failed. \nFd: %d, Value: %d, Error Msg: %s\n", 
                          fd, sval, strerror(errno));
  }
  return ret;
}

JNIEXPORT jint JNICALL
  Java_org_apache_hadoop_syscall_LinuxSystemCall_getSockOpt
(JNIEnv *env, jobject obj, jobject sock, jint level, jint opt)
{
  int fd, sval;
  fd = getFd(env, sock);

  socklen_t len = sizeof(int);
  int ret = getsockopt(fd, level, opt, (void *)&sval, &len);
  if (ret == 0) {
    return sval;
  } else {
    printf("getsockopt failed. \nFd: %d, Level: %d, Opt: %d, Error Msg: %s\n", 
                          fd, level, opt, strerror(errno));
    return ret;
  }
}

JNIEXPORT jint JNICALL
  Java_org_apache_hadoop_syscall_LinuxSystemCall_setSockOptBySocket
(JNIEnv *env, jobject obj, jobject sock, jint level, jint opt, jint val)
{
  int fd, sval;
  fd = getFdBySocket(env, sock);
  sval = val;

  int ret = setsockopt(fd, level, opt, (void *)&sval, sizeof(int));

  if (ret < 0) {
    printf("setsockopt failed. \nFd: %d, Value: %d, Error Msg: %s\n", 
                          fd, sval, strerror(errno));
  }
  return ret;
}

JNIEXPORT jint JNICALL
  Java_org_apache_hadoop_syscall_LinuxSystemCall_getSockOptBySocket
(JNIEnv *env, jobject obj, jobject sock, jint level, jint opt)
{
  int fd, sval;
  fd = getFdBySocket(env, sock);

  socklen_t len = sizeof(int);
  int ret = getsockopt(fd, level, opt, (void *)&sval, &len);
  if (ret == 0) {
    return sval;
  } else {
    printf("getsockopt failed. \nFd: %d, Level: %d, Opt: %d, Error Msg: %s\n", 
                          fd, level, opt, strerror(errno));
    return ret;
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_syscall_LinuxSystemCall_kill(
    JNIEnv *env, jclass class, jint pid, jint sig
    ) {
  jint ret;
  ret = kill(pid, sig);
  return ret;
}


