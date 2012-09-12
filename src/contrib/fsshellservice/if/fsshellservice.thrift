#!/usr/local/bin/thrift -java

namespace java org.apache.hadoop.hdfs.fsshellservice
namespace php fsshellservice
namespace py fsshellservice
namespace cpp fsshellservice

struct DfsFileStatus {
  1: required string path,
  2: required i64 length,
  3: required bool isdir,
  4: required i64 modification_time,
  5: required i64 access_time,
}

exception FsShellException {
  1: string message
}

exception FsShellFileNotFoundException {
  1: string message
}


service FsShellService
{
  void copyFromLocal(1:string src, 2:string dest)
      throws (1:FsShellException e),
  void copyToLocal(1:string src, 2:string dest)
      throws (1:FsShellException e),
  /**
   * remove() returns true only if the existing file or directory
   * was actually removed from the file system. remove() will return
   * false if the file doesn't exist ...
   */
  bool remove(1:string path, 2:bool recursive)
      throws (1:FsShellException e),
  /**
   * mkdirs() returns true if the operation succeeds.
   * This method silently succeeds if the directory already exists.
   * It will fail if a file by the given name exists. All path
   * elements in the given directory path will be silently created.
   * The behavior is similar to the Unix command mkdir -p.
   */
  bool mkdirs(1:string f)
      throws (1:FsShellException e),
  /**
   * rename() true if successful, or false if the old name does not
   * exist or if the new name already belongs to the namespace.
   */
  bool rename(1:string src, 2:string dest)
      throws (1:FsShellException e),
  list<DfsFileStatus> listStatus(1:string path)
      throws (1:FsShellException e, 2:FsShellFileNotFoundException efnf),
  DfsFileStatus getFileStatus(1:string path)
      throws (1:FsShellException e, 2:FsShellFileNotFoundException efnf),
  bool exists(1:string path)
      throws (1:FsShellException e),
}
