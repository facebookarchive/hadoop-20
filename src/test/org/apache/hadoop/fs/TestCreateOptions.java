package org.apache.hadoop.fs;

import junit.framework.TestCase;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

public class TestCreateOptions extends TestCase {

  @Test
  public void testBlockSize() {
    long blockSize = 2731234213L;
    CreateOptions.BlockSize opt = CreateOptions.blockSize(blockSize);
    assertEquals((Long)blockSize, opt.getValue());
  }
  
  @Test
  public void testBufferSize() {
    int bufferSize = 2731234;
    CreateOptions.BufferSize opt = CreateOptions.bufferSize(bufferSize);
    assertEquals((Integer)bufferSize, opt.getValue());
  }
  
  @Test
  public void testReplicationFactor() {
    short replicationFactor = 273;
    CreateOptions.ReplicationFactor opt = CreateOptions.replicationFactor(replicationFactor);
    assertEquals((Short)replicationFactor, opt.getValue());
  }
  
  @Test
  public void testBytesPerChecksum() {
    int bytes = 27315214;
    CreateOptions.BytesPerChecksum opt = CreateOptions.bytesPerChecksum(bytes);
    assertEquals((Integer)bytes, opt.getValue());
  }
  
  @Test
  public void testPerms() {
    FsPermission perm = FsPermission.getDefault();
    CreateOptions.Perms opt = CreateOptions.perms(perm);
    assertEquals(perm, opt.getValue());
  }
  
  @Test
  public void testProgress() {
    Progressable progress = new Progressable() {
      public void progress() {
      }
    };
    CreateOptions.Progress opt = CreateOptions.progress(progress);
    assertEquals(progress, opt.getValue());
  }
  
  /**
   * Please note that this method also checks new methods in WriteOptions as
   * well as CreateOptions.writeOptions(boolean, Boolean).
   */
  @Test
  public void testWriteOptions() {
    boolean overwrite = false;
    boolean forceSync = true;
    WriteOptions opt = CreateOptions.writeOptions(overwrite, forceSync);
    assertEquals(overwrite, ((WriteOptions)opt.getValue()).getOverwrite());
    assertEquals(forceSync, ((WriteOptions)opt.getValue()).getForceSync());
  }
  
  
  @Test
  public void testWriteOptionsDefaultValue() {
    WriteOptions opt = CreateOptions.writeOptions(null, null);
    assertEquals(true, ((WriteOptions)opt.getValue()).getOverwrite());
    assertEquals(false, ((WriteOptions)opt.getValue()).getForceSync());
  }
  
}
