#!/usr/local/bin/thrift -java

#
# Thrift Service exported by Hadoop File System Datanodes
# Dhruba Borthakur (dhruba@gmail.com)
#

/**
 * The available types in Thrift:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 */

namespace java org.apache.hadoop.thriftdatanode.api
namespace php hadoopdatanode
namespace py hadoopdatanode

// namespace id for hdfs
struct ThdfsNamespaceId {
  1:i32 id
}

// hdfs block object
struct ThdfsBlock {
  1:i64 blockId,
  2:i64 numBytes,
  3:i64 generationStamp
}

// hdfs block-path object
struct ThdfsBlockPath {
  1:string localBlockPath,
  2:string localMetaPath
}

struct TDatanodeID {
  1:string name       // host:port for datanode
}

exception MalformedInputException {
  1: string message
}

exception ThriftIOException {
  1: string message
}

service ThriftHadoopDatanode
{
  // Start generation-stamp recovery for specified block
  ThdfsBlock recoverBlock(1:TDatanodeID datanode,
                          2:ThdfsNamespaceId namespaceId,
                          3:ThdfsBlock block,
                          4:bool keepLength,
                          5:list<TDatanodeID> targets,
                          6:i64 deadline) 
                          throws (1:ThriftIOException ouch),


  // get block info from datanode
  ThdfsBlock getBlockInfo(1:TDatanodeID datanode,
                          2:ThdfsNamespaceId namespaceid, 
                          3:ThdfsBlock block)
                          throws (1:ThriftIOException ouch), 

  // Instruct the datanode to copy a block to specified target.
  void copyBlock(1:TDatanodeID datanode,
                 2:ThdfsNamespaceId srcNamespaceId, 3:ThdfsBlock srcblock,
                 4:ThdfsNamespaceId dstNamespaceId, 5:ThdfsBlock destBlock,
                 6:TDatanodeID target, 7:bool asynchronous) 
                 throws (1:ThriftIOException ouch),

  // Retrives the filename of the blockfile and the metafile from the datanode
  ThdfsBlockPath getBlockPathInfo(1:TDatanodeID datanode,
                                  2:ThdfsNamespaceId namespaceId, 
                                  3:ThdfsBlock block) 
                                  throws (1:ThriftIOException ouch),

}
