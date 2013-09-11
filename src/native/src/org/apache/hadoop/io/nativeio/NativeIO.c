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

// get the autoconf settings
#include "config.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <jni.h>
#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include "org_apache_hadoop.h"
#include "org_apache_hadoop_io_nativeio_NativeIO.h"
#include "file_descriptor.h"
#include "errno_enum.h"

// Copied from ioprio.h
#define IOPRIO_CLASS_SHIFT	(13)
#define IOPRIO_PRIO_VALUE(class, data)	(((class) << IOPRIO_CLASS_SHIFT) | data)
#define	IOPRIO_WHO_PROCESS (1)

// the NativeIO$Stat inner class and its constructor
static jclass stat_clazz;
static jmethodID stat_ctor;

// the NativeIOException class and its constructor
static jclass nioe_clazz;
static jmethodID nioe_ctor;

// the monitor used for working around non-threadsafe implementations
// of getpwuid_r, observed on platforms including RHEL 6.0.
// Please see HADOOP-7156 for details.
static jobject pw_lock_object;

// Internal functions
static void throw_ioe(JNIEnv* env, int errnum);
static ssize_t get_pw_buflen();

/**
 * Returns non-zero if the user has specified that the system
 * has non-threadsafe implementations of getpwuid_r or getgrgid_r.
 **/
static int workaround_non_threadsafe_calls(JNIEnv *env, jclass clazz) {
  jfieldID needs_workaround_field = (*env)->GetStaticFieldID(env, clazz,
    "workaroundNonThreadSafePasswdCalls", "Z");
  PASS_EXCEPTIONS_RET(env, 0);
  assert(needs_workaround_field);

  jboolean result = (*env)->GetStaticBooleanField(
    env, clazz, needs_workaround_field);
  return result;
}

static void stat_init(JNIEnv *env, jclass nativeio_class) {
  // Init Stat
  jclass clazz = (*env)->FindClass(env, "org/apache/hadoop/io/nativeio/NativeIO$Stat");
  PASS_EXCEPTIONS(env);
  stat_clazz = (*env)->NewGlobalRef(env, clazz);
  stat_ctor = (*env)->GetMethodID(env, stat_clazz, "<init>",
    "(Ljava/lang/String;Ljava/lang/String;IIJ)V");

  jclass obj_class = (*env)->FindClass(env, "java/lang/Object");
  assert(obj_class != NULL);
  jmethodID  obj_ctor = (*env)->GetMethodID(env, obj_class,
    "<init>", "()V");
  assert(obj_ctor != NULL);

  if (workaround_non_threadsafe_calls(env, nativeio_class)) {
    pw_lock_object = (*env)->NewObject(env, obj_class, obj_ctor);
    PASS_EXCEPTIONS(env);
    pw_lock_object = (*env)->NewGlobalRef(env, pw_lock_object);
    PASS_EXCEPTIONS(env);
  }
}

static void stat_deinit(JNIEnv *env) {
  if (stat_clazz != NULL) {
    (*env)->DeleteGlobalRef(env, stat_clazz);
    stat_clazz = NULL;
  }
  if (pw_lock_object != NULL) {
    (*env)->DeleteGlobalRef(env, pw_lock_object);
    pw_lock_object = NULL;
  }
}

static void nioe_init(JNIEnv *env) {
  // Init NativeIOException
  nioe_clazz = (*env)->FindClass(
    env, "org/apache/hadoop/io/nativeio/NativeIOException");
  PASS_EXCEPTIONS(env);

  nioe_clazz = (*env)->NewGlobalRef(env, nioe_clazz);
  nioe_ctor = (*env)->GetMethodID(env, nioe_clazz, "<init>",
    "(Ljava/lang/String;Lorg/apache/hadoop/io/nativeio/Errno;)V");
}

static void nioe_deinit(JNIEnv *env) {
  if (nioe_clazz != NULL) {
    (*env)->DeleteGlobalRef(env, nioe_clazz);
    nioe_clazz = NULL;
  }
  nioe_ctor = NULL;
}

/*
 * private static native void initNative();
 *
 * We rely on this function rather than lazy initialization because
 * the lazy approach may have a race if multiple callers try to
 * init at the same time.
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_initNative(
	JNIEnv *env, jclass clazz) {

  stat_init(env, clazz);
  PASS_EXCEPTIONS_GOTO(env, error);
  nioe_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  fd_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  errno_enum_init(env);
  PASS_EXCEPTIONS_GOTO(env, error);
  return;
error:
  // these are all idempodent and safe to call even if the
  // class wasn't initted yet
  stat_deinit(env);
  nioe_deinit(env);
  fd_deinit(env);
  errno_enum_deinit(env);
}

/*
 * Processes the result of stat() and creates the Java level
 * NativeIO$Stat object
 */
static jobject process_stat(JNIEnv *env, struct stat s) {

  jobject ret = NULL;
  char *pw_buf = NULL;
  int pw_lock_locked = 0;

  size_t pw_buflen = get_pw_buflen();
  if ((pw_buf = malloc(pw_buflen)) == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for pw buffer");
    goto cleanup;
  }

  if (pw_lock_object != NULL) {
    if ((*env)->MonitorEnter(env, pw_lock_object) != JNI_OK) {
      goto cleanup;
    }
    pw_lock_locked = 1;
  }

  // Grab username
  struct passwd pwd, *pwdp;
  int rc;
  while ((rc = getpwuid_r(s.st_uid, &pwd, pw_buf, pw_buflen, &pwdp)) != 0) {
    if (rc != ERANGE) {
      throw_ioe(env, rc);
      goto cleanup;
    }
    free(pw_buf);
    pw_buflen *= 2;
    if ((pw_buf = malloc(pw_buflen)) == NULL) {
      THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for pw buffer");
      goto cleanup;
    }
  }
  assert(pwdp == &pwd);

  jstring jstr_username = (*env)->NewStringUTF(env, pwd.pw_name);
  if (jstr_username == NULL) goto cleanup;

  // Grab group
  struct group grp, *grpp;
  while ((rc = getgrgid_r(s.st_gid, &grp, pw_buf, pw_buflen, &grpp)) != 0) {
    if (rc != ERANGE) {
      throw_ioe(env, rc);
      goto cleanup;
    }
    free(pw_buf);
    pw_buflen *= 2;
    if ((pw_buf = malloc(pw_buflen)) == NULL) {
      THROW(env, "java/lang/OutOfMemoryError", "Couldn't allocate memory for pw buffer");
      goto cleanup;
    }
  }
  assert(grpp == &grp);

  jstring jstr_groupname = (*env)->NewStringUTF(env, grp.gr_name);
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  // Construct result
  ret = (*env)->NewObject(env, stat_clazz, stat_ctor,
    jstr_username, jstr_groupname, s.st_mode, s.st_nlink, s.st_ino);

cleanup:
  if (pw_buf != NULL) free(pw_buf);
  if (pw_lock_locked) {
    (*env)->MonitorExit(env, pw_lock_object);
  }
  return ret;
}

/*
 * public static native Stat fstat(FileDescriptor fd);
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_fstat(
  JNIEnv *env, jclass clazz, jobject fd_object)
{
  jobject ret = NULL;

  int fd = fd_get(env, fd_object);

  struct stat s;
  int rc = fstat(fd, &s);
  if (rc != 0) {
    throw_ioe(env, errno);
    return ret;
  }
  return process_stat(env, s);
}

/*
 * public static native Stat stat(String path);
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_stat(
    JNIEnv *env, jclass class, jstring path) {
  jboolean isCopy;
  jobject jStat = NULL;
  const char *file = NULL;

  if (!path) {
    THROW(env, "java/io/IOException", "Invalid argument passed");
    goto cleanup;
  }

  file = (*env)->GetStringUTFChars(env, path, &isCopy);
  if (!file) {
    THROW(env, "java/io/IOException", "Invalid argument passed");
    goto cleanup;
  }

  // Retrieve result of stat()
  struct stat result;
  int ret = stat(file, &result);
  if (ret == -1) {
    throw_ioe(env, errno);
    goto cleanup;
  }

  jStat = process_stat(env, result);

cleanup:
  if (file != NULL) {
    (*env)->ReleaseStringUTFChars(env, path, file);
  }
  return jStat;
}

/**
 * public static native int ioprio_get() throws IOException;
 */
JNIEXPORT int JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_ioprio_1get(
  JNIEnv *env, jclass clazz) {
#ifndef SYS_ioprio_get
  THROW(env, "java/lang/UnsupportedOperationException",
        "ioprio_get support not available");
#else
  pid_t tid = syscall(SYS_gettid);
  int prio = syscall(SYS_ioprio_get, IOPRIO_WHO_PROCESS, tid);
  if(prio == -1) {
    if (errno == ENOSYS) {
      // we know the syscall number, but it's not compiled
      // into the running kernel
      THROW(env, "java/lang/UnsupportedOperationException",
          "ioprio_get kernel support not available");
    } else {
      THROW(env, "java/io/IOException", strerror(errno));
    }
  }
  return prio;
#endif
}

void syscall_ioprio_set(JNIEnv *env, jint ioprio_prio_value) {
  pid_t tid = syscall(SYS_gettid);
  if(syscall(SYS_ioprio_set, IOPRIO_WHO_PROCESS, tid,
        ioprio_prio_value) == -1) {
    if (errno == ENOSYS) {
      // we know the syscall number, but it's not compiled
      // into the running kernel
      THROW(env, "java/lang/UnsupportedOperationException",
          "ioprio_set kernel support not available");
    } else {
      THROW(env, "java/io/IOException", strerror(errno));
    }
  }
}

/**
 * public static native void ioprio_set(int ioprio_prio_value)
 * throws IOException;
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_ioprio_1set__I(
  JNIEnv *env, jclass clazz, jint ioprio_prio_value) {
#ifndef SYS_ioprio_set
  THROW(env, "java/lang/UnsupportedOperationException",
        "ioprio_set support not available");
#else
  syscall_ioprio_set(env, ioprio_prio_value);
#endif
}

/**
 * public static native void ioprio_set(int classOfService, int priority)
 * throws IOException;
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_ioprio_1set__II(
  JNIEnv *env, jclass clazz, jint class, jint priority) {
#ifndef SYS_ioprio_set
  THROW(env, "java/lang/UnsupportedOperationException",
        "ioprio_set support not available");
#else
  syscall_ioprio_set(env, IOPRIO_PRIO_VALUE(class, priority));
#endif
}


/**
 * public static native void posix_fadvise(
 *   FileDescriptor fd, long offset, long len, int flags);
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_posix_1fadvise(
  JNIEnv *env, jclass clazz,
  jobject fd_object, jlong offset, jlong len, jint flags)
{
#ifndef HAVE_POSIX_FADVISE
  THROW(env, "java/lang/UnsupportedOperationException",
        "fadvise support not available");
#else
  int fd = fd_get(env, fd_object);
  PASS_EXCEPTIONS(env);

  int err = 0;
  if ((err = posix_fadvise(fd, (off_t)offset, (off_t)len, flags))) {
    throw_ioe(env, err);
  }
#endif
}

#if defined(HAVE_SYNC_FILE_RANGE)
#  define my_sync_file_range sync_file_range
#elif defined(SYS_sync_file_range)
// RHEL 5 kernels have sync_file_range support, but the glibc
// included does not have the library function. We can
// still call it directly, and if it's not supported by the
// kernel, we'd get ENOSYS. See RedHat Bugzilla #518581
static int manual_sync_file_range (int fd, __off64_t from, __off64_t to, unsigned int flags)
{
#ifdef __x86_64__
  return syscall( SYS_sync_file_range, fd, from, to, flags);
#else
  return syscall (SYS_sync_file_range, fd,
    __LONG_LONG_PAIR ((long) (from >> 32), (long) from),
    __LONG_LONG_PAIR ((long) (to >> 32), (long) to),
    flags);
#endif
}
#define my_sync_file_range manual_sync_file_range
#endif

/**
 * public static native void sync_file_range(
 *   FileDescriptor fd, long offset, long len, int flags);
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_sync_1file_1range(
  JNIEnv *env, jclass clazz,
  jobject fd_object, jlong offset, jlong len, jint flags)
{
#ifndef my_sync_file_range
  THROW(env, "java/lang/UnsupportedOperationException",
        "sync_file_range support not available");
#else
  int fd = fd_get(env, fd_object);
  PASS_EXCEPTIONS(env);

  if (my_sync_file_range(fd, (off_t)offset, (off_t)len, flags)) {
    if (errno == ENOSYS) {
      // we know the syscall number, but it's not compiled
      // into the running kernel
      THROW(env, "java/lang/UnsupportedOperationException",
            "sync_file_range kernel support not available");
      return;
    } else {
      throw_ioe(env, errno);
    }
  }
#endif
}

/*
 * public static native void fsync(String path);
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_fsync(
  JNIEnv *env, jclass clazz, jstring j_path) {
  int fd = -1;
  const char *path = (*env)->GetStringUTFChars(env, j_path, NULL);
  if (path == NULL) {
    THROW(env, "java/io/IOException", "Invalid argument passed");
    goto cleanup;
  }

  fd = open(path, 0);
  if (fd == -1) {
    throw_ioe(env, errno);
    goto cleanup;
  }

  if (fsync(fd) == -1) {
    throw_ioe(env, errno);
    goto cleanup;
  }

cleanup:
  if (path != NULL) {
    (*env)->ReleaseStringUTFChars(env, j_path, path);
  }
  if (fd != -1) {
    if (close(fd) == -1) {
      throw_ioe(env, errno);
    }
  }
}

/*
 * public static native FileDescriptor open(String path, int flags, int mode);
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_open(
  JNIEnv *env, jclass clazz, jstring j_path,
  jint flags, jint mode)
{
  jobject ret = NULL;

  const char *path = (*env)->GetStringUTFChars(env, j_path, NULL);
  if (path == NULL) goto cleanup; // JVM throws Exception for us

  int fd;
  if (flags & O_CREAT) {
    fd = open(path, flags, mode);
  } else {
    fd = open(path, flags);
  }

  if (fd == -1) {
    throw_ioe(env, errno);
    goto cleanup;
  }

  ret = fd_create(env, fd);

cleanup:
  if (path != NULL) {
    (*env)->ReleaseStringUTFChars(env, j_path, path);
  }
  return ret;
}

/*
 * public static native void chmod(String path, int mode) throws IOException;
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_chmod(
  JNIEnv *env, jclass clazz, jstring j_path,
  jint mode)
{
  const char *path = (*env)->GetStringUTFChars(env, j_path, NULL);
  if (path == NULL) return; // JVM throws Exception for us

  if (chmod(path, mode) != 0) {
    throw_ioe(env, errno);
  }

  (*env)->ReleaseStringUTFChars(env, j_path, path);
}


/*
 * Throw a java.IO.IOException, generating the message from errno.
 */
static void throw_ioe(JNIEnv* env, int errnum)
{
  const char* message;
  char buffer[80];
  jstring jstr_message;

  buffer[0] = 0;
#ifdef STRERROR_R_CHAR_P
  // GNU strerror_r
  message = strerror_r(errnum, buffer, sizeof(buffer));
  assert (message != NULL);
#else
  int ret = strerror_r(errnum, buffer, sizeof(buffer));
  if (ret == 0) {
    message = buffer;
  } else {
    message = "Unknown error";
  }
#endif
  jobject errno_obj = errno_to_enum(env, errnum);

  if ((jstr_message = (*env)->NewStringUTF(env, message)) == NULL)
    goto err;

  jthrowable obj = (jthrowable)(*env)->NewObject(env, nioe_clazz, nioe_ctor,
    jstr_message, errno_obj);
  if (obj == NULL) goto err;

  (*env)->Throw(env, obj);
  return;

err:
  if (jstr_message != NULL)
    (*env)->ReleaseStringUTFChars(env, jstr_message, message);
}

/*
 * public static native void link(String src, String dst);
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_link(
  JNIEnv *env, jclass class, jstring jsrc, jstring jdst) {
  jboolean isCopy;

	if (!jsrc || !jdst) {
    THROW(env, "java/io/IOException", "Invalid argument passed");
    return;
	}

	const char *src = (*env)->GetStringUTFChars(env, jsrc, &isCopy);
	const char *dst = (*env)->GetStringUTFChars(env, jdst, &isCopy);
	if (!src || !dst) {
    THROW(env, "java/io/IOException", "Invalid argument passed");
    goto cleanup;
	}

  if (link(src, dst) == -1) {
    throw_ioe(env, errno);
  }

cleanup:
  if (src != NULL) {
    (*env)->ReleaseStringUTFChars(env, jsrc, src);
  }
  if (dst != NULL) {
    (*env)->ReleaseStringUTFChars(env, jsrc, dst);
  }
}

/*
 * public static native void clock_gettime(int which_clock, TimeSpec tp);
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_nativeio_NativeIO_clock_1gettime(JNIEnv *env,
                                                           jclass class,
                                                           jint which_clock,
                                                           jobject tp) {
  jfieldID tv_secFid, tv_nsecFid;
  jclass cls = NULL;
  struct timespec _tp;
  long rc;

  // check for null time
  if (!tp) {
    THROW(env, "java/io/IOException", "Invalid argument passed");
    return;
  }

  // perform linux call
  rc = clock_gettime(which_clock, &_tp);
  if(rc) {
    throw_ioe(env, errno);
    return;
  }

  // populate object with correct values
  cls = (*env)->GetObjectClass(env, tp);
  if(!cls) {
    THROW(env, "java/io/IOException", "GetObjectClass failed");
    return;
  }
  tv_secFid = (*env)->GetFieldID(env, cls, "tv_sec", "J");
  tv_nsecFid = (*env)->GetFieldID(env, cls, "tv_nsec", "J");

  if (tv_secFid == NULL || tv_nsecFid == NULL) {
    THROW(env, "java/io/IOException",
          "tp argument must have long members tv_sec and tv_nsec");
    return;
  }
  (*env)->SetLongField(env, tp, tv_secFid, _tp.tv_sec);
  (*env)->SetLongField(env, tp, tv_nsecFid, _tp.tv_nsec);
}


/*
 * Determine how big a buffer we need for reentrant getpwuid_r and getgrnam_r
 */
ssize_t get_pw_buflen() {
  size_t ret = 0;
  #ifdef _SC_GETPW_R_SIZE_MAX
  ret = sysconf(_SC_GETPW_R_SIZE_MAX);
  #endif
  return (ret > 512) ? ret : 512;
}
/**
 * vim: sw=2: ts=2: et:
 */

