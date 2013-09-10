#ifndef __linux__
#error This file may be compiled on Linux only.
#endif

#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/epoll.h>
#include "org_apache_hadoop_mapred_CGroupEventListener.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))


static int efd = -1;
static int cfd = -1;
static int event_control = -1;
static int epollFd_ = -1;
static struct epoll_event *events;

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_CGroupEventListener_init
  (JNIEnv * env, jclass inclass, jstring inctlFile)
{ 
  char event_control_path[PATH_MAX];
  char line[LINE_MAX];

  char * ctlfile = (char *)(*env)->GetStringUTFChars( env, inctlFile, NULL ) ;
  
  //setup the event files
  cfd = open(ctlfile, O_RDONLY);
  if (cfd == -1) {
    (*env)->ReleaseStringUTFChars(env, inctlFile, ctlfile);
    return -1;
  }

  if (snprintf(event_control_path, PATH_MAX, "%s/cgroup.event_control",
      dirname(ctlfile)) >= PATH_MAX) {
    (*env)->ReleaseStringUTFChars(env, inctlFile, ctlfile);
    return -2;
  }

  (*env)->ReleaseStringUTFChars(env, inctlFile, ctlfile);
  event_control = open(event_control_path, O_WRONLY);
  if (event_control == -1) {
    return -3;
  }

  efd = syscall(__NR_eventfd, 0, 0);
  if (efd == -1) {
    return -4;
  }
  
  if (snprintf(line, LINE_MAX, "%d %d", efd, cfd) >= LINE_MAX) {
    return -5;
  }

  if (write(event_control, line, strlen(line) + 1) == -1) {
    return -6;
  }

  epollFd_ = epoll_create(1);
  if (epollFd_ == -1) {
    return -7;
  }
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = efd;
  if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, efd, &ev) == -1) {
    return -8;
  }
  events = (struct epoll_event*) calloc(1, sizeof(struct epoll_event));
  if (events == NULL) {
    return -9;
  }

  return 0;
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_CGroupEventListener_waitForNotification
  (JNIEnv * env, jclass inclass, jint waitTime)
{
  uint64_t result;
  int i;
  int nfds = epoll_wait(epollFd_, events, 1, waitTime);
  for (i = 0; i < nfds; i++) {
    read(events[i].data.fd, &result, sizeof(result));
  }

  return nfds;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_CGroupEventListener_close
  (JNIEnv * env, jclass inclass)
{
  if (epollFd_ >= 0) {
    close(epollFd_);
  }
  if (efd >= 0) {
    close(efd);
  }
  if (event_control >= 0) {
    close(event_control);
  }
  if (cfd >= 0) {
    close(cfd);
  }
}
