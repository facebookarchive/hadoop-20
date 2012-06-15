package org.apache.hadoop.mapred;

import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.crypto.SecretKey;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.TaskTracker.ShuffleServerMetrics;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.FileRegion;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;

/**
 * Implements the shuffle logic via netty.
 */
public class ShuffleHandler extends SimpleChannelUpstreamHandler {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(ShuffleHandler.class);
  private final NettyMapOutputAttributes attributes;
  private int port;
  /** Metrics of all shuffles */
  private final ShuffleServerMetrics shuffleMetrics;


  public ShuffleHandler(NettyMapOutputAttributes attributes, int port) {
    this.attributes = attributes;
    this.port = port;
    shuffleMetrics = attributes.getShuffleServerMetrics();
  }

  private List<String> splitMaps(List<String> mapq) {
    if (null == mapq) {
      return null;
    }
    final List<String> ret = new ArrayList<String>();
    for (String s : mapq) {
      Collections.addAll(ret, s.split(","));
    }
    return ret;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent evt)
      throws Exception {
    HttpRequest request = (HttpRequest) evt.getMessage();
    if (request.getMethod() != GET) {
      sendError(ctx, METHOD_NOT_ALLOWED);
      return;
    }
    final Map<String,List<String>> q =
        new QueryStringDecoder(request.getUri()).getParameters();
    final List<String> mapIds = splitMaps(q.get("map"));
    final List<String> reduceQ = q.get("reduce");
    final List<String> jobQ = q.get("job");
    if (LOG.isDebugEnabled()) {
      LOG.debug("RECV: " + request.getUri() +
          "\n  mapId: " + mapIds +
          "\n  reduceId: " + reduceQ +
          "\n  jobId: " + jobQ);
    }

    if (mapIds == null || reduceQ == null || jobQ == null) {
      sendError(ctx, "Required param job, map and reduce", BAD_REQUEST);
      return;
    }
    if (reduceQ.size() != 1 || jobQ.size() != 1) {
      sendError(ctx, "Too many job/reduce parameters", BAD_REQUEST);
      return;
    }
    int reduceId;
    String jobId;
    try {
      reduceId = Integer.parseInt(reduceQ.get(0));
      jobId = jobQ.get(0);
    } catch (NumberFormatException e) {
      sendError(ctx, "Bad reduce parameter", BAD_REQUEST);
      return;
    } catch (IllegalArgumentException e) {
      sendError(ctx, "Bad job parameter", BAD_REQUEST);
      return;
    }
    final String reqUri = request.getUri();
    if (null == reqUri) {
      // TODO? add upstream?
      sendError(ctx, FORBIDDEN);
      return;
    }

    if (mapIds.size() > 1) {
      throw new IllegalArgumentException(
        "Doesn't support more than one map id.  Current requst is asking for "
        + mapIds.size());
    }

    Channel ch = evt.getChannel();
    // TODO refactor the following into the pipeline
    ChannelFuture lastMap = null;
    for (String mapId : mapIds) {
      try {
        lastMap =
            sendMapOutput(ctx, ch, jobId, mapId, reduceId);
        if (null == lastMap) {
          sendError(ctx, NOT_FOUND);
          return;
        }
      } catch (IOException e) {
        LOG.error("Shuffle error ", e);
        shuffleMetrics.failedOutput();
        sendError(ctx, e.getMessage(), INTERNAL_SERVER_ERROR);
        return;
      }
    }
    lastMap.addListener(ChannelFutureListener.CLOSE);
  }

  protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx, Channel ch,
      String jobId, String mapId, int reduce) throws IOException {
    LocalDirAllocator lDirAlloc = attributes.getLocalDirAllocator();
    FileSystem rfs = ((LocalFileSystem) attributes.getLocalFS()).getRaw();

    ShuffleServerMetrics shuffleMetrics = attributes.getShuffleServerMetrics();
    TaskTracker tracker = attributes.getTaskTracker();

    // Index file
    Path indexFileName = lDirAlloc.getLocalPathToRead(
        TaskTracker.getIntermediateOutputDir(jobId, mapId)
        + "/file.out.index", attributes.getJobConf());
    // Map-output file
    Path mapOutputFileName = lDirAlloc.getLocalPathToRead(
        TaskTracker.getIntermediateOutputDir(jobId, mapId)
        + "/file.out", attributes.getJobConf());

    /**
     * Read the index file to get the information about where
     * the map-output for the given reducer is available.
     */
    IndexRecord info = tracker.getIndexInformation(mapId, reduce,indexFileName);

    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

    //set the custom "from-map-task" http header to the map task from which
    //the map output data is being transferred
    response.setHeader(MRConstants.FROM_MAP_TASK, mapId);

    //set the custom "Raw-Map-Output-Length" http header to
    //the raw (decompressed) length
    response.setHeader(MRConstants.RAW_MAP_OUTPUT_LENGTH,
        Long.toString(info.rawLength));

    //set the custom "Map-Output-Length" http header to
    //the actual number of bytes being transferred
    response.setHeader(MRConstants.MAP_OUTPUT_LENGTH,
        Long.toString(info.partLength));

    //set the custom "for-reduce-task" http header to the reduce task number
    //for which this map output is being transferred
    response.setHeader(MRConstants.FOR_REDUCE_TASK, Integer.toString(reduce));

    ch.write(response);
    File spillfile = new File(mapOutputFileName.toString());
    RandomAccessFile spill;
    try {
      spill = new RandomAccessFile(spillfile, "r");
    } catch (FileNotFoundException e) {
      LOG.info(spillfile + " not found");
      return null;
    }
    final FileRegion partition = new DefaultFileRegion(
      spill.getChannel(), info.startOffset, info.partLength);
    ChannelFuture writeFuture = ch.write(partition);
    writeFuture.addListener(new ChanneFutureListenerMetrics(partition));
    shuffleMetrics.outputBytes(info.partLength); // optimistic
    LOG.info("Sending out " + info.partLength + " bytes for reduce: " +
             reduce + " from map: " + mapId + " given " +
             info.partLength + "/" + info.rawLength);
    return writeFuture;
  }

  /**
   * Handles the completion of the channel future operation.
   *
   * TODO: error handling; distinguish IO/connection failures,
   *       attribute to appropriate spill output
   */
  private class ChanneFutureListenerMetrics implements ChannelFutureListener {
    /** Will clean up resources on this when done */
    private final FileRegion partition;

    /**
     * Constructor.
     *
     * @param partition Release resources on this when done
     */
    private ChanneFutureListenerMetrics(FileRegion partition) {
      this.partition = partition;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      partition.releaseExternalResources();
      shuffleMetrics.successOutput();
    }
  }

  private void sendError(ChannelHandlerContext ctx,
      HttpResponseStatus status) {
    sendError(ctx, "", status);
  }

  private void sendError(ChannelHandlerContext ctx, String message,
      HttpResponseStatus status) {
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
    response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
    response.setContent(
        ChannelBuffers.copiedBuffer(message, CharsetUtil.UTF_8));

    // Close the connection as soon as the error message is sent.
    ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    Channel ch = e.getChannel();
    Throwable cause = e.getCause();
    if (cause instanceof TooLongFrameException) {
      sendError(ctx, BAD_REQUEST);
      return;
    }

    LOG.error("Shuffle error: ", cause);
    shuffleMetrics.failedOutput();
    if (ch.isConnected()) {
      LOG.error("Shuffle error " + e);
      sendError(ctx, INTERNAL_SERVER_ERROR);
    }
  }

}
