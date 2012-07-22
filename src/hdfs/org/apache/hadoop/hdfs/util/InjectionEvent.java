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
package org.apache.hadoop.hdfs.util;

/** 
 * Enumeration of all injection events.
 * When defining new events, please PREFIX the name 
 * with the supervised class.
 * 
 * Please see InjectionHandler.
 */
public enum InjectionEvent {
  
  FSIMAGE_STARTING_SAVE_NAMESPACE,
  FSIMAGE_CREATING_SAVER_THREADS,
  FSIMAGE_STARTING_SAVER_THREAD,
  FSIMAGE_SN_CLEANUP,
  FSIMAGE_CANCEL_REQUEST_RECEIVED,

  STANDBY_CANCELLED_EXCEPTION_THROWN,
  STANDBY_FELL_BEHIND,
  STANDBY_INSTANTIATE_INGEST, 
  STANDBY_QUIESCE_INGEST, 
  STANDBY_ENTER_CHECKPOINT, 
  STANDBY_EXIT_CHECKPOINT,
  STANDBY_BEFORE_SAVE_NAMESPACE, 
  STANDBY_BEFORE_PUT_IMAGE,
  STANDBY_BEFORE_ROLL_EDIT, 
  STANDBY_BEFORE_ROLL_IMAGE, 
  STANDBY_BEGIN_RUN, 
  STANDBY_INTERRUPT,
  STANDBY_EDITS_NOT_EXISTS,
  STANDBY_CREATE_INGEST_RUNLOOP,
  STANDBY_AFTER_DO_CHECKPOINT,

  INGEST_BEFORE_LOAD_EDIT,
  INGEST_READ_OP,
  
  OFFERSERVICE_SCHEDULE_HEARTBEAT,
  OFFERSERVICE_SCHEDULE_BR,
  
  AVATARNODE_CHECKEDITSTREAMS,
  AVATARNODE_SHUTDOWN,
  AVATARNODE_AFTER_STALE_CHECKPOINT_CHECK,

  AVATARDATANODE_BEFORE_START_OFFERSERVICE1,
  AVATARDATANODE_START_OFFERSERVICE1,
  AVATARDATANODE_START_OFFERSERVICE2,
  
  AVATARXEIVER_RUNTIME_FAILURE,

  NAMENODE_AFTER_CREATE_FILE,

  AVATARZK_GET_REGISTRATION_TIME,
  AVATARZK_GET_PRIMARY_ADDRESS,

  DAFS_CHECK_FAILOVER
}
