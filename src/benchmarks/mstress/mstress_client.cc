/**
 * $Id$
 *
 * Author: Thilee Subramaniam
 *
 * Copyright 2012 Quantcast Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * This C++ client performs filesystem meta opetarions on the QFS metaserver
 * using qfs_client.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <algorithm>

using namespace std;

#include "KfsClient.h"

FILE* logFile = stdout;

#define TEST_BASE_DIR "/mstress"
#define COUNT_INCR 500

/*
  This program is invoked with the following arguments:
    - qfs server/port
    - test name ('create', 'stat', or 'readdir')
    - a planfile
    - keys to read the planfile (hostname and process name)

  eg: For the following plan file,
  ---------------------------------------------
  #File or directory
  type=file
  #Number of levels in created tree
  levels=2
  #Number of inodes per level
  inodes=3
  #Number of random leaf paths to stat, per client
  nstat=17
  ---------------------------------------------
  if 'create' testname, '127.0.0.1' hostname, and 'proc_00' processname
  'PPP' path_prefix are given, then the created tree would be:
  /mstress/127.0.0.1_proc_00/PPP_0/PPP_0
  /mstress/127.0.0.1_proc_00/PPP_0/PPP_1
  /mstress/127.0.0.1_proc_00/PPP_0/PPP_2
  /mstress/127.0.0.1_proc_00/PPP_1/PPP_0
  /mstress/127.0.0.1_proc_00/PPP_1/PPP_1
  /mstress/127.0.0.1_proc_00/PPP_1/PPP_2
  /mstress/127.0.0.1_proc_00/PPP_2/PPP_0
  /mstress/127.0.0.1_proc_00/PPP_2/PPP_1
  /mstress/127.0.0.1_proc_00/PPP_2/PPP_2
*/


//Global datastructure to hold various options.
struct Client {
  static const size_t INITIAL_SIZE;
  struct Path
  {
    char* actualPath_;
    size_t actualSize_;
    size_t len_;

    Path() : actualSize_(INITIAL_SIZE), len_(0) {
      actualPath_ = (char*)calloc(actualSize_, 1);
    }

    void Push(const char* leafStr) {
      size_t leafLen = strlen(leafStr);
      if (leafLen == 0) {
        return;
      }
      if (len_ + 1 + leafLen + 1 > actualSize_) {
        actualSize_ *= 2;
        actualPath_ = (char*)realloc(actualPath_, actualSize_);
        fprintf(logFile, "Reallocating to %zu bytes\n", actualSize_);
      }
      if (leafStr[0] != '/') {
        strcpy(actualPath_ + len_, "/");
        len_ ++;
      }
      strcpy(actualPath_ + len_, leafStr);
      len_ += leafLen;
    }

    void Pop(const char* leafStr) {
      size_t leafLen = strlen(leafStr);
      if (leafLen > len_ - 1 ||
          strncmp(actualPath_ + len_ - leafLen, leafStr, leafLen)) {
        fprintf(logFile, "Error in pop %s from %s\n", leafStr, actualPath_);
        exit(0);
      }
      len_ -= leafLen + 1;
      *(actualPath_ + len_) = 0;
    }

    void Reset() {
      actualPath_[0] = 0;
      len_ = 0;
    }
  };

  Path path_;

  //from commadline
  string dfsServer_;
  int dfsPort_;
  string testName_;
  string planfilePath_;
  string prefix_;
  size_t prefixLen_;
  string hostName_;
  string processName_;

  //from planfile
  string type_;
  int levels_;
  int inodesPerLevel_;
  int pathsToStat_;
};
const size_t Client::INITIAL_SIZE = 1 << 12;

//A simple AutoDoUndo class to delete kfsClient automatically.
class AutoCleanupKfsClient
{
public:
  AutoCleanupKfsClient(Client* client) : initialized(false)
  {
    kfsClient = KFS::Connect(client->dfsServer_, client->dfsPort_);
    if (kfsClient) {
      initialized = true;
    }
  }
  ~AutoCleanupKfsClient() {
    delete kfsClient;
  }
  bool IsInitialized() { return initialized; }
  KFS::KfsClient* GetClient() { return kfsClient; }

private:
  KFS::KfsClient* kfsClient;
  bool initialized;
};


void Usage(const char* argv0)
{
  fprintf(logFile, "Usage: %s -s dfs-server -p dfs-port [-t [create|stat|readdir|delete] -a planfile-path -c host -n process-name -P path-prefix]\n", argv0);
  fprintf(logFile, "   -t: this option requires -a, -c, and -n options.\n");
  fprintf(logFile, "   -P: the default value is PATH_.\n");
  fprintf(logFile, "eg:\n%s -s <metaserver-host> -p <metaserver-port> -t create -a <planfile> -c localhost -n Proc_00\n", argv0);
  exit(0);
}

void hexout(char* str, int len) {
  for (int i = 0; i < len; i++) {
    printf("%02X ", str[i]);
  }
  printf("\n");
  for (int i = 0; i < len; i++) {
    printf("%2c ", str[i] < 30 ? '.' : str[i]);
  }
  printf("\n");
}

void myitoa(int n, char* buf)
{
  static char result[32];
  snprintf(result, 32, "%d", n);
  strcpy(buf, result);
}

//Return a random permutation of numbers in [0..range).
void unique_random(vector<size_t>& result, size_t range)
{
  result.resize(range);
  srand(time(NULL));

  for(size_t i=0; i<range; ++i) {
    result[i] = i;
  }

  random_shuffle(result.begin(), result.end() ) ;
}

void parse_options(int argc, char* argv[], Client* client)
{
  int c = 0;
  char *dfs_server = NULL,
       *dfs_port = NULL,
       *test_name = NULL,
       *planfile = NULL,
       *host = NULL,
       *prefix = NULL,
       *process_name = NULL;

  while ((c = getopt(argc, argv, "s:p:a:c:n:t:P:h")) != -1) {
    switch (c) {
      case 's':
        dfs_server = optarg;
        break;
      case 'p':
        dfs_port = optarg;
        break;
      case 'a':
        planfile = optarg;
        break;
      case 'c':
        host = optarg;
        break;
      case 'n':
        process_name = optarg;
        break;
      case 't':
        test_name = optarg;
        break;
        break;
      case 'P':
        prefix = optarg;
        break;
      case 'h':
      case '?':
        Usage(argv[0]);
        break;
      default:
        fprintf (logFile, "?? getopt returned character code 0%o ??\n", c);
        Usage(argv[0]);
        break;
      }
  }

  if (!client || !dfs_server || !dfs_port || !planfile || !host || !process_name || !test_name) {
    Usage(argv[0]);
  }
  ifstream ifs(planfile, ifstream::in);
  if (ifs.fail()) {
    fprintf(stdout, "Error: planfile not found\n");
    Usage(argv[0]);
  }
  ifs.close();

  client->testName_ = test_name;
  client->dfsServer_ = dfs_server;
  client->dfsPort_ = atoi(dfs_port);
  client->planfilePath_ = planfile;
  client->hostName_ = host;
  client->processName_ = process_name;
  if (!prefix) {
    client->prefix_ = "PATH_";
  } else {
    client->prefix_ = prefix;
  }
  client->prefixLen_ = client->prefix_.size();

  fprintf(logFile, "server=%s\n", dfs_server);
  fprintf(logFile, "port=%s\n", dfs_port);
  fprintf(logFile, "planfile=%s\n", planfile);
  fprintf(logFile, "host=%s\n", host);
  fprintf(logFile, "process_name=%s\n", process_name);
  fprintf(logFile, "test name=%s\n", test_name);
}

//Reads the plan file and add the level information to distribution_ vector.
//Also set type_, prefix_, levels_, pathsToStat_ class variables.
void ParsePlanFile(Client* client)
{
  string line;
  ifstream ifs(client->planfilePath_.c_str(), ifstream::in);

  while (ifs.good()) {
    getline(ifs, line);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    if (line.substr(0, 5) == "type=") {
      client->type_ = line.substr(5);
      continue;
    }
    if (line.substr(0, 7) == "levels=") {
      client->levels_ = atoi(line.substr(7).c_str());
      continue;
    }
    if (line.substr(0, 7) == "inodes=") {
      client->inodesPerLevel_ = atoi(line.substr(7).c_str());
      continue;
    }
    if (line.substr(0, 6) == "nstat=") {
      client->pathsToStat_ = atoi(line.substr(6).c_str());
      continue;
    }
  }
  ifs.close();
  if (client->levels_ <= 0 || client->inodesPerLevel_ <= 0 || client->type_.empty()) {
    fprintf(logFile, "Error parsing plan file\n");
    exit(-1);
  }
}

long TimeDiffMilliSec(struct timeval* alpha, struct timeval* zigma)
{
  long diff = 0;
  diff += (zigma->tv_sec - alpha->tv_sec) * 1000;
  diff += (zigma->tv_usec - alpha->tv_usec) / 1000;
  return diff < 0 ? 0 : diff;
}


int CreateDFSPaths(Client* client, AutoCleanupKfsClient* kfs, int level, int* createdCount)
{

  KFS::KfsClient* kfsClient = kfs->GetClient();
  int rc;
  bool isLeaf = (level + 1 >= client->levels_);
  bool isDir =isLeaf ? (client->type_ == "dir") : true;
  char name[512];
  strncpy(name, client->prefix_.c_str(), 512);
  for (int i = 0; i < client->inodesPerLevel_; i++) {
    myitoa(i, name + client->prefixLen_);
    client->path_.Push(name);
    //hexout(client->path_.actualPath_, client->path_.len_ + 3);

    if (isDir) {
      //fprintf(logFile, "Creating DIR [%s]\n", client->path_.actualPath_);
      rc = kfsClient->Mkdir(client->path_.actualPath_);
      if (rc < 0) {
        fprintf(logFile, "Mkdir(%s) failed with rc=%d\n", client->path_.actualPath_, rc);
        return rc;
      }
      (*createdCount)++;
      if (*createdCount > 0 && (*createdCount) % COUNT_INCR == 0) {
        fprintf(logFile, "Created paths so far: %d\n", *createdCount);
      }
      if (!isLeaf) {
        rc = CreateDFSPaths(client, kfs, level+1, createdCount);
        if (rc < 0) {
          fprintf(logFile, "CreateDFSPaths(%s) failed with rc=%d\n", client->path_.actualPath_, rc);
          return rc;
        }
      }
    } else {
      //fprintf(logFile, "Creating file [%s]\n", client->path_.actualPath_);
      rc = kfsClient->Create(client->path_.actualPath_);
      if (rc < 0) {
        fprintf(logFile, "Create(%s) failed with rc=%d\n", client->path_.actualPath_, rc);
        return rc;
      }
      (*createdCount)++;
      if (*createdCount > 0 && (*createdCount) % COUNT_INCR == 0) {
        fprintf(logFile, "Created paths so far: %d\n", *createdCount);
      }
    }
    client->path_.Pop(name);
  }
  return 0;
}

int CreateDFSPaths(Client* client, AutoCleanupKfsClient* kfs)
{
  KFS::KfsClient* kfsClient = kfs->GetClient();
  ostringstream os;
  os << TEST_BASE_DIR << "/" << client->hostName_ + "_" << client->processName_;
  int err = kfsClient->Mkdirs(os.str().c_str());
  //fprintf(logFile, "first mkdir err = %d\n", err);
  if (err && err != -EEXIST) {
    fprintf(logFile, "Error: mkdir test base dir failed\n");
    exit(-1);
  }

  int rc = 0;
  int createdCount = 0;
  struct timeval tvAlpha;
  gettimeofday(&tvAlpha, NULL);

  client->path_.Reset();
  client->path_.Push(os.str().c_str());
  if ((rc = CreateDFSPaths(client, kfs, 0, &createdCount)) < 0) {
    fprintf(logFile, "Error: failed to create DFS paths\n");
    return rc;
  }

  struct timeval tvZigma;
  gettimeofday(&tvZigma, NULL);
  fprintf(logFile, "Client: %d paths created in %ld msec\n", createdCount, TimeDiffMilliSec(&tvAlpha, &tvZigma));
  return 0;
}

int StatDFSPaths(Client* client, AutoCleanupKfsClient* kfs) {
  KFS::KfsClient* kfsClient = kfs->GetClient();

  ostringstream os;
  os << TEST_BASE_DIR << "/" << client->hostName_ + "_" << client->processName_;

  srand(time(NULL));
  struct timeval tvAlpha;
  gettimeofday(&tvAlpha, NULL);

  for (int count = 0; count < client->pathsToStat_; count++) {
    client->path_.Reset();
    client->path_.Push(os.str().c_str());
    char name[4096];
    strncpy(name, client->prefix_.c_str(), client->prefixLen_);

    for (int d = 0; d < client->levels_; d++) {
      int randIdx = rand() % client->inodesPerLevel_;
      myitoa(randIdx, name + client->prefixLen_);
      client->path_.Push(name);
      //fprintf(logFile, "Stat: path now is %s\n", client->path_.actualPath_);
    }
    //fprintf(logFile, "Stat: doing stat on [%s]\n", client->path_.actualPath_);

    KFS::KfsFileAttr attr;
    int err = kfsClient->Stat(os.str().c_str(), attr);
    if (err) {
      fprintf(logFile, "error doing stat on %s\n", os.str().c_str());
      return err;
    }

    if (count > 0 && count % COUNT_INCR == 0) {
      fprintf(logFile, "Stat paths so far: %d\n", count);
    }
  }

  struct timeval tvZigma;
  gettimeofday(&tvZigma, NULL);
  fprintf(logFile, "Client: Stat done on %d paths in %ld msec\n", client->pathsToStat_, TimeDiffMilliSec(&tvAlpha, &tvZigma));

  return 0;
}

int ListDFSPaths(Client* client, AutoCleanupKfsClient* kfs) {
  KFS::KfsClient* kfsClient = kfs->GetClient();

  srand(time(NULL));
  struct timeval tvAlpha;
  gettimeofday(&tvAlpha, NULL);
  int inodeCount = 0;

  queue<string> pending;
  ostringstream os;
  os << TEST_BASE_DIR << "/" << client->hostName_ + "_" << client->processName_;
  pending.push(os.str());

  while (!pending.empty()) {
    string parent = pending.front();
    pending.pop();
    //fprintf(logFile, "readdir on parent [%s]\n", parent.c_str());
    vector<KFS::KfsFileAttr> children;
    int err = kfsClient->ReaddirPlus(parent.c_str(), children);
    if (err) {
      fprintf(logFile, "Error [err=%d] reading directory %s\n", err, parent.c_str());
      return err;
    }
    while (!children.empty()) {
      string child = children.back().filename;
      bool isDir = children.back().isDirectory;
      children.pop_back();
      //fprintf(logFile, "  Child = %s inodeCount=%d\n", child.c_str(), inodeCount);
      if (child == "." ||
          child == "..") {
        continue;
      }
      inodeCount ++;
      if (isDir) {
        string nextParent = parent + "/" + child;
        pending.push(nextParent);
        //fprintf(logFile, "  Adding next parent [%s]\n", nextParent.c_str());
      }
      if (inodeCount > 0 && inodeCount % COUNT_INCR == 0) {
        fprintf(logFile, "Readdir paths so far: %d\n", inodeCount);
      }
    }
  }

  struct timeval tvZigma;
  gettimeofday(&tvZigma, NULL);
  fprintf(logFile, "Client: Directory walk done over %d inodes in %ld msec\n", inodeCount, TimeDiffMilliSec(&tvAlpha, &tvZigma));
  return 0;
}

int RemoveDFSPaths(Client* client, AutoCleanupKfsClient* kfs) {
  KFS::KfsClient* kfsClient = kfs->GetClient();
  KFS::KfsFileAttr attr;

  ostringstream os;
  os << TEST_BASE_DIR << "/" << client->hostName_ + "_" << client->processName_;

  // get a list of leaf indices to remove. Note that this is different from the nodename suffix.
  // eg: if we have 3 levels of 4 inodes, then there will be pow(4,3) = 64 leaf nodes. We are
  //     interested in a subset of indices in (0..63). This gets filled in 'leafIdxRangeForDel'.
  long double countLeaf = pow(client->inodesPerLevel_, client->levels_);
  vector<size_t> leafIdxRangeForDel;
  unique_random(leafIdxRangeForDel, countLeaf);
  fprintf(logFile, "To delete %zu paths\n", leafIdxRangeForDel.size());

  int err = 0;
  struct timeval tvAlpha;
  gettimeofday(&tvAlpha, NULL);
  bool isLeafDir = client->type_=="dir";

  char sfx[32];
  string pathToDel;
  string pathSoFar;
  size_t idx = 0;
  size_t pos = 0;
  int delta = 0;
  int lev = 0;

  while (!leafIdxRangeForDel.empty()) {
    idx = leafIdxRangeForDel.back();
    leafIdxRangeForDel.pop_back();
    pathSoFar.clear();
    pos = 0;
    delta = 0;
    lev = 0;
    while (lev < client->levels_) {
      pos = idx / client->inodesPerLevel_;
      delta = idx - (pos * client->inodesPerLevel_);
      myitoa(delta, sfx);
      if (pathSoFar.length()) {
        pathSoFar = client->prefix_ + sfx + "/" + pathSoFar;
      } else {
        pathSoFar = client->prefix_ + sfx;
      }
      idx = pos;
      lev ++;
    }

    pathToDel = os.str() + "/" + pathSoFar;
    //fprintf(logFile, "Client: Deleting %s ...\n", pathToDel.c_str());
    if (isLeafDir) {
      err = kfsClient->Rmdir(pathToDel.c_str());
      if (err) {
        fprintf(logFile, "Error [err=%d] deleting directory %s\n", err, pathToDel.c_str());
        return err;
      }
    } else {
      err = kfsClient->Remove(pathToDel.c_str());
      if (err) {
        fprintf(logFile, "Error [err=%d] deleting file %s\n", err, pathToDel.c_str());
        return err;
      }
    }
  }

  err = kfsClient->RmdirsFast(os.str().c_str());
  if (err) {
    fprintf(logFile, "Error [err=%d] RmdirsFast(%s)\n", err, os.str().c_str());
    return err;
  }

  struct timeval tvZigma;
  gettimeofday(&tvZigma, NULL);
  fprintf(logFile, "Client: Deleted %s. Delete took %ld msec\n", os.str().c_str(), TimeDiffMilliSec(&tvAlpha, &tvZigma));

  return 0;
}


int main(int argc, char* argv[])
{
  Client client;

  parse_options(argc, argv, &client);

  AutoCleanupKfsClient kfs(&client);
  if (!kfs.IsInitialized()) {
    fprintf(logFile, "kfs client failed to initialize. exiting.\n");
    exit(-1);
  }

  ParsePlanFile(&client);

  int result = 0;
  if (client.testName_ == "create") {
    result = CreateDFSPaths(&client, &kfs);
  } else if (client.testName_ == "stat") {
    result = StatDFSPaths(&client, &kfs);
  } else if (client.testName_ == "readdir") {
    result = ListDFSPaths(&client, &kfs);
  } else if (client.testName_ == "delete") {
    result = RemoveDFSPaths(&client, &kfs);
  } else {
    fprintf(logFile, "Error: unrecognized test '%s'", client.testName_.c_str());
    return -1;
  }
  return result;
}

