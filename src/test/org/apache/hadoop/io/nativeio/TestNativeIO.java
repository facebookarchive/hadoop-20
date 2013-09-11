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
package org.apache.hadoop.io.nativeio;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assume.*;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.NativeCodeLoader;

public class TestNativeIO {
  static final Log LOG = LogFactory.getLog(TestNativeIO.class);

  static final File TEST_DIR = new File(
    System.getProperty("test.build.data"), "testnativeio");

  private static final Random random = new Random();

  @Before
  public void checkLoaded() {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
  }

  @Before
  public void setupTestDir() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
    TEST_DIR.mkdirs();
  }

  @Test
  public void testStat() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat"));
    NativeIO.Stat stat = NativeIO.fstat(fos.getFD());
    fos.close();
    LOG.info("Stat: " + String.valueOf(stat));

    assertEquals(System.getProperty("user.name"), stat.getOwner());
    assertNotNull(stat.getGroup());
    assertTrue(!"".equals(stat.getGroup()));
    assertEquals("Stat mode field should indicate a regular file",
      NativeIO.Stat.S_IFREG, stat.getMode() & NativeIO.Stat.S_IFMT);
  }

  /**
   * Test for races in fstat usage
   *
   * NOTE: this test is likely to fail on RHEL 6.0 which has a non-threadsafe
   * implementation of getpwuid_r.
   */
  @Test
  public void testMultiThreadedStat() throws Exception {
    final FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat"));

    final AtomicReference<Throwable> thrown =
      new AtomicReference<Throwable>();
    List<Thread> statters = new ArrayList<Thread>();
    for (int i = 0; i < 10; i++) {
      Thread statter = new Thread() {
        public void run() {
          long et = System.currentTimeMillis() + 5000;
          while (System.currentTimeMillis() < et) {
            try {
              NativeIO.Stat stat = NativeIO.fstat(fos.getFD());
              assertEquals(System.getProperty("user.name"), stat.getOwner());
              assertNotNull(stat.getGroup());
              assertTrue(!"".equals(stat.getGroup()));
              assertEquals("Stat mode field should indicate a regular file",
                NativeIO.Stat.S_IFREG, stat.getMode() & NativeIO.Stat.S_IFMT);
            } catch (Throwable t) {
              thrown.set(t);
            }
          }
        }
      };
      statters.add(statter);
      statter.start();
    }
    for (Thread t : statters) {
      t.join();
    }

    fos.close();
    
    if (thrown.get() != null) {
      throw new RuntimeException(thrown.get());
    }
  }

  @Test
  public void testStatClosedFd() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat2"));
    fos.close();
    try {
      NativeIO.Stat stat = NativeIO.fstat(fos.getFD());
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
  }

  @Test
  public void testOpenMissingWithoutCreate() throws Exception {
    LOG.info("Open a missing file without O_CREAT and it should fail");
    try {
      FileDescriptor fd = NativeIO.open(
        new File(TEST_DIR, "doesntexist").getAbsolutePath(),
        NativeIO.O_WRONLY, 0700);
      fail("Able to open a new file without O_CREAT");
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.ENOENT, nioe.getErrno());
    }
  }

  @Test
  public void testOpenWithCreate() throws Exception {
    LOG.info("Test creating a file with O_CREAT");
    FileDescriptor fd = NativeIO.open(
      new File(TEST_DIR, "testWorkingOpen").getAbsolutePath(),
      NativeIO.O_WRONLY | NativeIO.O_CREAT, 0700);
    assertNotNull(true);
    assertTrue(fd.valid());
    FileOutputStream fos = new FileOutputStream(fd);
    fos.write("foo".getBytes());
    fos.close();

    assertFalse(fd.valid());

    LOG.info("Test exclusive create");
    try {
      fd = NativeIO.open(
        new File(TEST_DIR, "testWorkingOpen").getAbsolutePath(),
        NativeIO.O_WRONLY | NativeIO.O_CREAT | NativeIO.O_EXCL, 0700);
      fail("Was able to create existing file with O_EXCL");
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception for failed exclusive create", nioe);
      assertEquals(Errno.EEXIST, nioe.getErrno());
    }
  }

  /**
   * Test that opens and closes a file 10000 times - this would crash with
   * "Too many open files" if we leaked fds using this access pattern.
   */
  @Test
  public void testFDDoesntLeak() throws IOException {
    for (int i = 0; i < 10000; i++) {
      FileDescriptor fd = NativeIO.open(
        new File(TEST_DIR, "testNoFdLeak").getAbsolutePath(),
        NativeIO.O_WRONLY | NativeIO.O_CREAT, 0700);
      assertNotNull(true);
      assertTrue(fd.valid());
      FileOutputStream fos = new FileOutputStream(fd);
      fos.write("foo".getBytes());
      fos.close();
    }
  }

  /**
   * Test basic chmod operation
   */
  @Test
  public void testChmod() throws Exception {
    try {
      NativeIO.chmod("/this/file/doesnt/exist", 777);
      fail("Chmod of non-existent file didn't fail");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.ENOENT, nioe.getErrno());
    }

    File toChmod = new File(TEST_DIR, "testChmod");
    assertTrue("Create test subject",
               toChmod.exists() || toChmod.mkdir());
    NativeIO.chmod(toChmod.getAbsolutePath(), 0777);
    assertPermissions(toChmod, 0777);
    NativeIO.chmod(toChmod.getAbsolutePath(), 0000);
    assertPermissions(toChmod, 0000);
    NativeIO.chmod(toChmod.getAbsolutePath(), 0644);
    assertPermissions(toChmod, 0644);
  }

  /**
   * Test basic fsync operations.
   */
  @Test
  public void testFsync() throws Exception {
    // Test fsync on invalid dir.
    File testDir = new File(TEST_DIR, "testfsyncdir");
    testDir.deleteOnExit();
    try {
      NativeIO.fsync(testDir.getAbsolutePath());
      fail("Did not throw exception");
    } catch (IOException ie) {
      LOG.info("Got expected exception", ie);
    }

    // Test fsync on valid dir.
    assertTrue(testDir.mkdirs());
    NativeIO.fsync(testDir.getAbsolutePath());

    // Test fsync on file.
    File testF = new File(TEST_DIR, "testfsyncfile");
    testF.deleteOnExit();
    testF.createNewFile();
    NativeIO.fsync(testF.getAbsolutePath());
  }

  @Test
  public void testPosixFadvise() throws Exception {
    FileInputStream fis = new FileInputStream("/dev/zero");
    try {
      NativeIO.posix_fadvise(fis.getFD(), 0, 0,
                             NativeIO.POSIX_FADV_SEQUENTIAL);
    } catch (UnsupportedOperationException uoe) {
      // we should just skip the unit test on machines where we don't
      // have fadvise support
      assumeTrue(false);
    } finally {
      fis.close();
    }

    try {
      NativeIO.posix_fadvise(fis.getFD(), 0, 1024,
                             NativeIO.POSIX_FADV_SEQUENTIAL);

      fail("Did not throw on bad file");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
    
    try {
      NativeIO.posix_fadvise(null, 0, 1024,
                             NativeIO.POSIX_FADV_SEQUENTIAL);

      fail("Did not throw on null file");
    } catch (NullPointerException npe) {
      // expected
    }
  }

  @Test
  public void testSyncFileRange() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testSyncFileRange"));
    try {
      fos.write("foo".getBytes());
      NativeIO.sync_file_range(fos.getFD(), 0, 1024,
                               NativeIO.SYNC_FILE_RANGE_WRITE);
      // no way to verify that this actually has synced,
      // but if it doesn't throw, we can assume it worked
    } catch (UnsupportedOperationException uoe) {
      // we should just skip the unit test on machines where we don't
      // have fadvise support
      assumeTrue(false);
    } finally {
      fos.close();
    }
    try {
      NativeIO.sync_file_range(fos.getFD(), 0, 1024,
                               NativeIO.SYNC_FILE_RANGE_WRITE);
      fail("Did not throw on bad file");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
  }

  private void verifyStatAfterHardLink(File f, long inode) throws IOException {
    NativeIO.Stat stat = NativeIO.stat(f);
    assertNotNull(stat);
    assertTrue(stat.getInode() > 0);
    assertEquals(inode, stat.getInode());
    assertEquals(2, stat.getHardLinks());
  }

  @Test
  public void testSimpleStat() throws IOException {
    File src = new File("src" + random.nextInt());
    try {
      assertTrue(src.createNewFile());
      NativeIO.Stat stat = NativeIO.stat(src);
      assertNotNull(stat);
      assertTrue(stat.getInode() > 0);
      assertEquals(1, stat.getHardLinks());
    } finally {
      src.delete();
    }
  }

  @Test
  public void testStatWithHardLinks() throws IOException {
    File src = new File("src" + random.nextInt());
    File dst = new File("dst" + random.nextInt());
    try {
      assertTrue(src.createNewFile());
      NativeIO.Stat stat = NativeIO.stat(src);
      long inode = stat.getInode();
      NativeIO.link(src, dst);
      verifyStatAfterHardLink(src, inode);
      verifyStatAfterHardLink(dst, inode);
    } finally {
      src.delete();
      dst.delete();
    }
  }

  @Test
  public void testStatNonExistentFile() throws IOException {
    File src = new File("nonexistentfile");
    try {
      NativeIO.Stat stat = NativeIO.stat(src);
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.ENOENT, nioe.getErrno());
      return;
    }
    fail("No exception thrown");
  }

  @Test
  public void testStatFileTooLong() throws IOException {
    String fileName = "";
    for (int i = 0; i < 1000; i++) {
      fileName += i;
    }
    File src = new File(fileName);
    try {
      NativeIO.Stat stat = NativeIO.stat(src);
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.ENAMETOOLONG, nioe.getErrno());
      return;
    }
    fail("No exception thrown");
  }

  @Test
  public void testStatNullFile() throws IOException {
    try {
      String filePath = null;
      NativeIO.Stat stat = NativeIO.stat(filePath);
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      return;
    }
    fail("No exception throw");
  }

  @Test
  public void testHardLinkNonExistentSrc() throws IOException {
    File src = new File("nonexistentfile" + random.nextInt());
    try {
      NativeIO.link(src, new File("tmp"));
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.ENOENT, nioe.getErrno());
      return;
    }
    fail("No Exception thrown");
  }

  @Test
  public void testHardLinkExistingDst() throws IOException {
    File src = new File("src" + random.nextInt());
    File dst = new File("dst" + random.nextInt());
    try {
      assertTrue(src.createNewFile());
      assertTrue(dst.createNewFile());
      NativeIO.link(src, dst);
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.EEXIST, nioe.getErrno());
      return;
    } finally {
      src.delete();
      dst.delete();
    }
    fail("No Exception thrown");
  }

  @Test
  public void testHardLinkNullFile() throws IOException {
    try {
      NativeIO.link(null, new File("tmp"));
    } catch (IllegalArgumentException e) {
      LOG.info("Got expected exception", e);
      return;
    }
    fail("No Exception thrown");
  }

  @Test
  public void testHardLinkNullFileNative() throws IOException {
    try {
      String src = null;
      String dst = "tmp";
      NativeIO.link(src , dst);
    } catch (IOException e) {
      LOG.info("Got expected exception", e);
      return;
    }
    fail("No Exception thrown");
  }

  @Test
  public void testClockGetTimeNative() throws IOException {
    // get first time
    NativeIO.TimeSpec t1 = new NativeIO.TimeSpec();
    NativeIO.clock_gettime(NativeIO.CLOCK_REALTIME, t1);
    if(t1.tv_sec == 0 && t1.tv_nsec == 0)
      fail("clock_gettime returned 0 for the time.");

    // get second time
    NativeIO.TimeSpec t2 = new NativeIO.TimeSpec();
    NativeIO.clock_gettime(NativeIO.CLOCK_REALTIME, t2);
    if(t2.tv_sec == 0 && t2.tv_nsec == 0)
      fail("clock_gettime returned 0 for the time.");

    // make sure time hasn't gone backwards
    if(t2.tv_sec < t1.tv_sec ||
       (t2.tv_sec == t1.tv_sec && t2.tv_nsec < t1.tv_nsec))
      fail("2nd call to clock_gettime returned smaller value than 1s call");
  }

  @Test
  public void testClockGetTimeNull() throws IOException {
    try {
      NativeIO.clock_gettime(NativeIO.CLOCK_REALTIME, null);
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      return;
    }
    fail("No exception throw");
  }

  @Test
  public void testClockGetTimeWithInvalidClock() throws IOException {
    try {
      // use a garbage number (i.e., 53715) for which_clock
      NativeIO.clock_gettime(53715, null);
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      return;
    }
    fail("No exception throw");
  }

  @Test
  public void testClockGetTimeEachClock() throws IOException {
    NativeIO.TimeSpec t = new NativeIO.TimeSpec();
    NativeIO.clock_gettime(NativeIO.CLOCK_REALTIME, t);
    NativeIO.clock_gettime(NativeIO.CLOCK_MONOTONIC, t);
    NativeIO.clock_gettime(NativeIO.CLOCK_PROCESS_CPUTIME_ID, t);
    NativeIO.clock_gettime(NativeIO.CLOCK_THREAD_CPUTIME_ID, t);
    NativeIO.clock_gettime(NativeIO.CLOCK_MONOTONIC_RAW, t);
    NativeIO.clock_gettime(NativeIO.CLOCK_REALTIME_COARSE, t);
    NativeIO.clock_gettime(NativeIO.CLOCK_MONOTONIC_COARSE, t);
  }

  @Test
  public void testClockGetTimeIfPossible() throws IOException {
    // get first time
    NativeIO.TimeSpec t1 = new NativeIO.TimeSpec();
    NativeIO.clockGetTimeIfPossible(NativeIO.CLOCK_REALTIME, t1);
    if(t1.tv_sec == 0 && t1.tv_nsec == 0)
      fail("clockGetTimeIfPossible returned 0 for the time.");

    // get second time
    NativeIO.TimeSpec t2 = new NativeIO.TimeSpec();
    NativeIO.clockGetTimeIfPossible(NativeIO.CLOCK_REALTIME, t2);
    if(t2.tv_sec == 0 && t2.tv_nsec == 0)
      fail("clockGetTimeIfPossible returned 0 for the time.");

    // make sure time hasn't gone backwards
    if(t2.tv_sec < t1.tv_sec ||
       (t2.tv_sec == t1.tv_sec && t2.tv_nsec < t1.tv_nsec))
      fail("2nd call to clockGetTimeIfPossible returned "+
           "smaller value than 1s call");
  }

  @Test
  public void testFileRangeSync() throws IOException {
    File testFile = new File(TEST_DIR, "fileRangeSync");
    testFile.deleteOnExit();
    FileOutputStream out = new FileOutputStream(testFile);
    try {
      byte[] buffer = new byte[8096];
      random.nextBytes(buffer);
      out.write(buffer);
      NativeIO.syncFileRangeIfPossible(out.getFD(), 0, 8096,
          NativeIO.SYNC_FILE_RANGE_WRITE);
    } finally {
      out.close();
    }
  }

  @Test
  public void testIoprioSet() throws IOException {
    // TODO : figure out how to test IOPRIO_CLASS_RT and IOPRIO_CLASS_IDLE,
    // currently they need CAP_SYS_ADMIN capablilities for the current process.
    for (int i = 0; i < 8; i++) {
      NativeIO.ioprio_set(NativeIO.IOPRIO_CLASS_BE, i);
    }
  }

  @Test
  public void testIoprioSet1() throws IOException {
    for (int i = 0; i < 8; i++) {
      NativeIO.ioprio_set((NativeIO.IOPRIO_CLASS_BE << 13) | i);
    }
  }

  @Test
  public void testIoprioSetGet() throws IOException {
    NativeIO.ioprio_set(NativeIO.IOPRIO_CLASS_BE, 5);
    assertEquals((NativeIO.IOPRIO_CLASS_BE << 13) | 5,
        NativeIO.ioprio_get());
  }

  @Test(expected=IOException.class)
  public void testIoprioSet1Err1() throws IOException {
    NativeIO.ioprio_set((NativeIO.IOPRIO_CLASS_BE << 13) | (-1));
  }

  @Test(expected=IOException.class)
  public void testIoprioSetErr1() throws IOException {
    NativeIO.ioprio_set(NativeIO.IOPRIO_CLASS_BE, -1);
  }

  @Test(expected=IOException.class)
  public void testIoprioSetErr2() throws IOException {
    // Only 0-7 is allowed.
    NativeIO.ioprio_set(NativeIO.IOPRIO_CLASS_BE, 8);
  }

  @Test(expected=IOException.class)
  public void testIoprioSetErr3() throws IOException {
    NativeIO.ioprio_set(-1, 1);
  }

  @Test(expected=IOException.class)
  public void testIoprioSetErr4() throws IOException {
    // only 0-3 class of service supported.
    NativeIO.ioprio_set(4, 1);
  }

  private void assertPermissions(File f, int expected) throws IOException {
    FileSystem localfs = FileSystem.getLocal(new Configuration());
    FsPermission perms = localfs.getFileStatus(
      new Path(f.getAbsolutePath())).getPermission();
    assertEquals(expected, perms.toShort());
  }
}
