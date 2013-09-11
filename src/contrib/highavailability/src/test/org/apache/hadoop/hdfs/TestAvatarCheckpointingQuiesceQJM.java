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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.TestAvatarCheckpointing.TestAvatarCheckpointingHandler;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.junit.Test;

public class TestAvatarCheckpointingQuiesceQJM extends
		TestAvatarCheckpointingQuiesce {
	
	@Test
  public void testQuiescingWhenDoingCheckpoint1() throws Exception {
    TestAvatarCheckpointing.testQuiesceInterruption(
        InjectionEvent.STANDBY_INSTANTIATE_INGEST, true, false, true);
  }
	
	@Test
  public void testQuiescingWhenDoingCheckpoint2() throws Exception {
    TestAvatarCheckpointing.testQuiesceInterruption(
        InjectionEvent.STANDBY_QUIESCE_INGEST, true, false, true);
  }
	
	@Test
  public void testQuiescingWhenDoingCheckpoint3() throws Exception {
    // quiesce comes before rolling the log for checkpoint,
    // the log will be closed on standby but not opened
    TestAvatarCheckpointing.testQuiesceInterruption(
        InjectionEvent.STANDBY_ENTER_CHECKPOINT, true, true, true);
  }
	
	@Test
  public void testQuiescingWhenDoingCheckpoint4() throws Exception {
    // quiesce comes before rolling the log for checkpoint,
    // the log will be closed on standby but not opened
    TestAvatarCheckpointing.testQuiesceInterruption(
        InjectionEvent.STANDBY_BEFORE_ROLL_EDIT, true, true, true);
  }
	
	@Test
  public void testQuiescingWhenDoingCheckpoint5() throws Exception {
    TestAvatarCheckpointing.testQuiesceInterruption(
        InjectionEvent.STANDBY_BEFORE_SAVE_NAMESPACE, true, false, true);
  }
	
	@Test
  public void testQuiescingWhenDoingCheckpoint6() throws Exception {
    // this one does not throw cancelled exception, quiesce after SN
    TestAvatarCheckpointing.testQuiesceInterruption(
        InjectionEvent.STANDBY_BEFORE_PUT_IMAGE, false, false, true);
  }
	
	@Test
  public void testQuiescingBeforeCheckpoint() throws Exception {
    // quiesce comes before rolling the log for checkpoint,
    // the log will be closed on standby but not opened
    TestAvatarCheckpointing.testQuiesceInterruption(
        InjectionEvent.STANDBY_BEGIN_RUN, true, true, true);
  }
	
	@Test
  public void testQuiescingImageValidationCreation() throws Exception {
    // test if creation of new validation fails after standby quiesce
    TestAvatarCheckpointingHandler h = TestAvatarCheckpointing
        .testQuiesceInterruption(InjectionEvent.STANDBY_VALIDATE_CREATE, false,
            false, true);
    assertTrue(h.receivedEvents
        .contains(InjectionEvent.STANDBY_VALIDATE_CREATE_FAIL));
  }
	
	@Test
  public void testQuiescingImageUpload() throws Exception {
    // test if the image upload is interrupted when quiescing standby
    TestAvatarCheckpointingHandler h = TestAvatarCheckpointing
        .testQuiesceInterruption(InjectionEvent.STANDBY_UPLOAD_CREATE,
            InjectionEvent.STANDBY_QUIESCE_INTERRUPT, false, false, false, true);
    assertTrue(h.receivedEvents.contains(InjectionEvent.STANDBY_UPLOAD_FAIL));
  }
	
	@Test
  public void testQuiescingImageValidationInterruption() throws Exception {
    // test if an ongoing image validation is interrupted
    TestAvatarCheckpointingHandler h = TestAvatarCheckpointing
        .testQuiesceInterruption(InjectionEvent.IMAGE_LOADER_CURRENT_START,
            InjectionEvent.STANDBY_QUIESCE_INTERRUPT, false, false, false, true);
    assertTrue(h.receivedEvents
        .contains(InjectionEvent.IMAGE_LOADER_CURRENT_INTERRUPT));
  }
}
