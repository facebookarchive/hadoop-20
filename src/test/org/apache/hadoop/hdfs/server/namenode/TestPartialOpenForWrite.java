package org.apache.hadoop.hdfs.server.namenode;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestFileAppend4;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.BlockMetaInfoType;
import org.apache.log4j.Logger;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class TestPartialOpenForWrite extends TestCase {
  private static final Logger LOG = Logger.getLogger(TestPartialOpenForWrite.class);
  private static final Answer IDENTITY_ANSWER = new Answer() {
    @Override
    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
      return invocationOnMock.callRealMethod();
    }
  };

  private MiniDFSCluster cluster;
  private FileSystem fileSystem;
  private Configuration conf;
  private int numDatanodes = 1;
  private CallbackAnswer commitBlockSyncAnswer;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    conf = new Configuration();
    conf.setInt("ipc.client.connect.max.retries", 0);
    cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitClusterUp();
    fileSystem = cluster.getFileSystem();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    cluster.shutdown();
  }

  private CallbackAnswer createAppendSpy()
    throws IOException {
    final FSNamesystem spyNamesystem = spy(cluster.getNameNode().namesystem);
    cluster.getNameNode().namesystem = spyNamesystem;
    CallbackAnswer answer = new CallbackAnswerBuilder(
      new ExectionThrowingCallback(IOException.class, 1))
      .setCallbackBefore(false)
      .setDelayBefore(false)
      .build();

    doAnswer(answer)
      .when(spyNamesystem)
      .appendFile(anyString(), anyString(), anyString(), (BlockMetaInfoType)anyObject());

    setupCommitBlockSyncAnswer(spyNamesystem);

    return answer;
  }

  private CallbackAnswer createCreateSpy()
    throws IOException {
    FSNamesystem spyNamesystem = spy(cluster.getNameNode().namesystem);
    cluster.getNameNode().namesystem = spyNamesystem;
    CallbackAnswer answer = new CallbackAnswerBuilder(
      new ExectionThrowingCallback(IOException.class, 1)
    )
      .setCallbackBefore(false)
      .setDelayBefore(false)
      .build();
    doAnswer(answer)
      .when(spyNamesystem)
      .startFile(
        anyString(),
        (PermissionStatus) anyObject(),
        anyString(),
        anyString(),
        anyBoolean(),
        anyBoolean(),
        anyShort(),
        anyLong()
      );

    return answer;
  }

  private void setupCommitBlockSyncAnswer(FSNamesystem spyNamesystem)
    throws IOException {
    commitBlockSyncAnswer = new CallbackAnswerBuilder()
      .setDelayBefore(false)
      .build();
    doAnswer(commitBlockSyncAnswer)
      .when(spyNamesystem)
      .commitBlockSynchronization(
        (Block) anyObject(),
        anyLong(),
        anyLong(),
        anyBoolean(),
        anyBoolean(),
        (DatanodeID[]) anyObject()
      );
  }

  public void testAppendPartialFailure() throws Exception {
    doPartialOpenForWriteFailure(new AppendFileOperation(2));
  }

  public void testCreatePartialFailure() throws Exception {
    doPartialOpenForWriteFailure(new CreateFileOperation());
  }

  public void doPartialOpenForWriteFailure(FileOperation fileOperation)
    throws Exception {
    Path path = new Path("/fuu");

    fileOperation.setup(path);

    try {
      fileOperation.perform(path);
      fail("expected exception doing file operation");
    } catch (IOException e) {
      LOG.info("initial operation failed, trying second operation");
      fileOperation.perform(path);
      LOG.info("success!");
    } 
  }

  private void createFile(Path path) throws IOException {
    FSDataOutputStream out = fileSystem.create(path);
    byte[] buf = DFSTestUtil.generateSequentialBytes(0, 64 * 1024);
    out.write(buf);
    out.close();
  }

  private interface FileOperation {
    public void setup(Path path) throws IOException;
    public void perform(Path path) throws IOException;
  }

  private class AppendFileOperation implements FileOperation {
    private final int waitOnCall;
    
    private int numCalls = 0;
    private CallbackAnswer appendFileAnswer;

    private AppendFileOperation(int waitOnCall) {
      this.waitOnCall = waitOnCall;
    }

    @Override
    public void setup(Path path) throws IOException {
      createFile(path);
      appendFileAnswer = createAppendSpy();
      appendFileAnswer.proceed();
    }

    @Override
    public void perform(Path path) throws IOException {
      if (++numCalls >= waitOnCall) {
        try {
          // wait 30s at most for lease recovery
          commitBlockSyncAnswer.waitForCall(30000);
          commitBlockSyncAnswer.proceed();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }

      fileSystem.append(path);
    }
  }

  private class CreateFileOperation implements FileOperation {
    private CallbackAnswer startFileAnswer;

    @Override
    public void setup(Path path) throws IOException {
      startFileAnswer = createCreateSpy();
      startFileAnswer.proceed();
    }

    @Override
    public void perform(Path path) throws IOException {
      fileSystem.create(path);
    }
  }

  private static interface Callback {
    void execute() throws Throwable;
  }

  private static class ExectionThrowingCallback implements Callback {
    private final Class<? extends Throwable> exceptionClass;
    private final int numTimes;

    private int count = 0;

    private ExectionThrowingCallback(Class<? extends Throwable> exceptionClass, int numTimes) {
      this.exceptionClass = exceptionClass;
      this.numTimes = numTimes;
    }

    @Override
    public void execute() throws Throwable {
      if (++count <= numTimes) {
        throw exceptionClass.newInstance();
      }
    }
  }

  private static class CallbackAnswer extends TestFileAppend4.DelayAnswer {
    private final boolean doBefore;
    private final Callback callback;

    private CallbackAnswer(
      Callback callback,
      boolean callbackBefore,
      boolean delayBefore,
      int numTimesBeforeDelay
    ) {
      super(delayBefore, numTimesBeforeDelay);
      this.doBefore = callbackBefore;
      this.callback = callback;
    }

    @Override
    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
      if (doBefore) {
        callback.execute();
      }

      Object result = super.answer(invocationOnMock);

      if (!doBefore) {
        callback.execute();
      }

      return result;
    }
  }

  private class CallbackAnswerBuilder {
    private boolean delayBefore = true;
    private boolean callbackBefore = true;
    private int numTimesBeforeDelay = 1;

    private final Callback callback;

    private CallbackAnswerBuilder() {
      this(new Callback() {
        @Override
        public void execute() throws Throwable {
        }
      });
    }

    private CallbackAnswerBuilder(Callback callback) {
      this.callback = callback;
    }

    public CallbackAnswerBuilder setDelayBefore(boolean delayBefore) {
      this.delayBefore = delayBefore;

      return this;
    }

    public CallbackAnswerBuilder setCallbackBefore(boolean callbackBefore) {
      this.callbackBefore = callbackBefore;

      return this;
    }

    public CallbackAnswerBuilder setNumTimesBeforeDelay(int numTimesBeforeDelay) {
      this.numTimesBeforeDelay = numTimesBeforeDelay;

      return this;
    }

    CallbackAnswer build() {
      return new CallbackAnswer(
        callback, callbackBefore, delayBefore, numTimesBeforeDelay
      );
    }
  }

}
