package org.apache.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.log4j.Level;

/**
 * Unittest for HftpFileSystem.
 * 
 */
public class TestHftpFileSystem extends TestCase {
  private static final Random RAN = new Random();
  private static final Path TEST_FILE = new Path("/testfile+1");
  
  private static Configuration config = null;
  private static MiniDFSCluster cluster = null;
  private static FileSystem hdfs = null;
  private static HftpFileSystem hftpFs = null;
  
  /**
   * Setup hadoop mini-cluster for test.
   */
  private static void oneTimeSetUp() throws IOException {
    ((Log4JLogger)HftpFileSystem.LOG).getLogger().setLevel(Level.ALL);

    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    config = new Configuration();
    config.set(FSConstants.SLAVE_HOST_NAME, "localhost");

    cluster = new MiniDFSCluster(config, 2, true, null);
    hdfs = cluster.getFileSystem();
    final String hftpuri = "hftp://" + config.get("dfs.http.address");
    System.out.println("hftpuri=" + hftpuri);
    hftpFs = (HftpFileSystem) new Path(hftpuri).getFileSystem(config);
  }
  
  /**
   * Shutdown the hadoop mini-cluster.
   */
  private static void oneTimeTearDown() throws IOException {
    hdfs.close();
    hftpFs.close();
    cluster.shutdown();
  }
  
  public TestHftpFileSystem(String name) {
    super(name);
  }

  /**
   * For one time setup / teardown.
   */
  public static Test suite() {
    TestSuite suite = new TestSuite();
    
    suite.addTestSuite(TestHftpFileSystem.class);
    
    return new TestSetup(suite) {
      @Override
      protected void setUp() throws IOException {
        oneTimeSetUp();
      }
      
      @Override
      protected void tearDown() throws IOException {
        oneTimeTearDown();
      }
    };
  }
  
  /**
   * Tests isUnderConstruction() functionality.
   */
  public void testIsUnderConstruction() throws Exception {
    // Open output file stream.
    FSDataOutputStream out = hdfs.create(TEST_FILE, true);
    out.writeBytes("test");
    
    // Test file under construction.
    FSDataInputStream in1 = hftpFs.open(TEST_FILE);
    assertTrue(in1.isUnderConstruction());
    in1.close();
    
    // Close output file stream.
    out.close();
    
    // Test file not under construction.
    FSDataInputStream in2 = hftpFs.open(TEST_FILE);
    assertFalse(in2.isUnderConstruction());
    in2.close();
  }
  
  /**
   * Tests getPos() functionality.
   */
  public void testGetPos() throws Exception {
    // Write a test file.
    FSDataOutputStream out = hdfs.create(TEST_FILE, true);
    out.writeBytes("0123456789");
    out.close();
    
    FSDataInputStream in = hftpFs.open(TEST_FILE);
    
    // Test read().
    for (int i = 0; i < 5; ++i) {
      assertEquals(i, in.getPos());
      in.read();
    }
    
    // Test read(b, off, len).
    assertEquals(5, in.getPos());
    byte[] buffer = new byte[10];
    assertEquals(2, in.read(buffer, 0, 2));
    assertEquals(7, in.getPos());
    
    // Test read(b).
    int bytesRead = in.read(buffer);
    assertEquals(7 + bytesRead, in.getPos());
    
    // Test EOF.
    for (int i = 0; i < 100; ++i) {
      in.read();
    }
    assertEquals(10, in.getPos());
    in.close();
  }
  
  /**
   * Tests seek().
   */
  public void testSeek() throws Exception {
    // Write a test file.
    FSDataOutputStream out = hdfs.create(TEST_FILE, true);
    out.writeBytes("0123456789");
    out.close();
    
    FSDataInputStream in = hftpFs.open(TEST_FILE);
    in.seek(7);
    assertEquals('7', in.read());
  }
  
  /**
   * Scenario: Read an under construction file using hftp.
   * 
   * Expected: Hftp should be able to read the latest byte after the file
   * has been hdfsSynced (but not yet closed).
   * 
   * @throws IOException
   */
  public void testConcurrentRead() throws IOException {
    // Write a test file.
    FSDataOutputStream out = hdfs.create(TEST_FILE, true);
    out.writeBytes("123");
    out.sync();  // sync but not close
    
    // Try read using hftp.
    FSDataInputStream in = hftpFs.open(TEST_FILE);
    assertEquals('1', in.read());
    assertEquals('2', in.read());
    assertEquals('3', in.read());
    in.close();
    
    // Try seek and read.
    in = hftpFs.open(TEST_FILE);
    in.seek(2);
    assertEquals('3', in.read());
    in.close();
    
    out.close();
  }
  
  public void testPrematureEOS() throws IOException, URISyntaxException {
    try {
      readHftpFile(true, true);
      fail("did not get expected EOFException");
    } catch (Exception e) {
      assertTrue(
        String.format("got [%s] instead of expected EOFException", e),
        e instanceof EOFException
      );
    } 
  }

  public void testMissingContentLengthWhenStrict() 
    throws IOException, URISyntaxException {
    try {
      readHftpFile(true, false);
      fail("did not get expected IOException");      
    } catch (Exception e) {
      assertTrue(
        String.format("got [%s] instead of expected IOException", e),         
        e instanceof IOException && 
          e.getMessage().toLowerCase().contains("missing")
      );
    }
  }

  public void testPrematureEOSCompatible1() throws IOException, URISyntaxException {
    try {
      readHftpFile(false, false);
    } catch (Exception e) {
      fail(String.format("unexpected exception [%s]", e));
    }
  }

  public void testPrematureEOSCompatible2() throws IOException, URISyntaxException {
    try {
      readHftpFile(false, true);
      fail("did not get expected EOFException");
    } catch (Exception e) {
      assertTrue(
        String.format("got [%s] instead of expected EOFException", e),
        e instanceof EOFException
      );
    } 
  }

  public void readHftpFile(
    boolean strictContentLength, boolean sendContentLength
  )
    throws IOException, URISyntaxException {
    int bufSize = 128 * 1024;
    byte[] buf = DFSTestUtil.generateSequentialBytes(0, bufSize);
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
    final long contentLength = bufSize + 1;
    Configuration conf = new Configuration();
  
    conf.setBoolean(HftpFileSystem.STRICT_CONTENT_LENGTH, strictContentLength);
  
    HftpFileSystem fileSystem =
      new MockHftpFileSystem(
        sendContentLength ? contentLength : null, inputStream, conf
      );
    FSDataInputStream dataInputStream = fileSystem.open(new Path("dont-care"));
    byte[] readBuf = new byte[1024];

    while (dataInputStream.read(readBuf) > -1) {
      //nothing
    }

    dataInputStream.close();
  }

  private static class MockHftpFileSystem extends HftpFileSystem {
    private final Long contentLength;
    private final ByteArrayInputStream inputStream;

    MockHftpFileSystem(Long contentLength, ByteArrayInputStream inputStream)
      throws IOException, URISyntaxException {
      this(contentLength, inputStream, new Configuration());
    }
    
    MockHftpFileSystem(
      Long contentLength, ByteArrayInputStream inputStream, Configuration conf
    ) throws IOException, URISyntaxException {
      this.contentLength = contentLength;
      this.inputStream = inputStream;
      this.initialize(new URI("hftp://localhost:1234"), conf);
    }

    @Override
    protected HttpURLConnection openConnection(String path, String query)
      throws IOException {
      return new HttpURLConnection(null){
        @Override
        public String getHeaderField(String name) {
          if (HftpFileSystem.CONTENT_LENGTH_FIELD.equals(name) &&
            contentLength != null) {
            return String.valueOf(contentLength);
          } else {
            return super.getHeaderField(name);
          }
        }

        @Override
        public InputStream getInputStream() throws IOException {
          return inputStream; 
        }

        @Override
        public void disconnect() {
        }

        @Override
        public boolean usingProxy() {
          return false;
        }

        @Override
        public void connect() throws IOException {
        }
      };
    }
  }
}
