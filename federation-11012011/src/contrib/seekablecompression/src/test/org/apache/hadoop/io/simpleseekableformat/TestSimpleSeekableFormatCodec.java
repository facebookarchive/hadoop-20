package org.apache.hadoop.io.simpleseekableformat;

import java.io.OutputStream;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPrematureEOFException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class TestSimpleSeekableFormatCodec extends TestCase {

  /**
   * Test the SeekableFileFormatCodec with TextInputFormat.
   */
  public void testTextInputFormat() throws Exception {
    testTextInputFormat(null);
    testTextInputFormat(GzipCodec.class);
  }

  public void testTextInputFormat(final Class<? extends CompressionCodec> codecClass) throws Exception {
    // Try basic test with no truncation
    testTextInputFormat(codecClass, 1, 0, -1);
    testTextInputFormat(codecClass, 1000, 0, -1);
    testTextInputFormat(codecClass, 1000 * 1000, 0, -1);

    // Try truncate the file at different positions
    // SimpleSeekableFormat should read partial files without any exceptions.
    testTextInputFormat(codecClass, 1000 * 1000, 0, 0);
    testTextInputFormat(codecClass, 1000 * 1000, 0, 11);
    testTextInputFormat(codecClass, 1000 * 1000, 0, 1024);
    testTextInputFormat(codecClass, 1000 * 1000, 0, 500 * 1000);
    testTextInputFormat(codecClass, 1000 * 1000, 0, 1024 * 1024);
    testTextInputFormat(codecClass, 1000 * 1000, 0, 1024 * 1024 + 100);
    testTextInputFormat(codecClass, 1000 * 1000, 0, 1024 * 1024 + 1024);
    testTextInputFormat(codecClass, 1000 * 1000, 0, 1024 * 1024 + 50 * 1000);

    // Try flush every 100 records
    testTextInputFormat(codecClass, 1000 * 1000, 500, 500 * 1000);
    testTextInputFormat(codecClass, 1000 * 1000, 500, 1024 * 1024 + 50 * 1000);
  }

  String getRecord(int recordId) {
    return "" + recordId;
  }

  /**
   * @param truncateSize  < 0 means do not truncate the file.
   */
  @SuppressWarnings("deprecation")
  void testTextInputFormat(final Class<? extends CompressionCodec> codecClass,
      final int numRecords, final int flushRecords, final int truncateSize) throws Exception {

    System.out.println("STARTING: numRecords=" + numRecords + " flushRecords=" + flushRecords
        + " truncateSize=" + truncateSize);

    final String fileName = System.getProperty("user.dir") + "/test.ssf";
    Path path = new Path(fileName);

    Configuration conf = new Configuration();
    FileSystem fs = LocalFileSystem.getLocal(conf);

    // Set codec class for compression inside the .ssf file.
    if (codecClass != null) {
      conf.setClass(SimpleSeekableFormat.FILEFORMAT_SSF_CODEC_CONF, codecClass, CompressionCodec.class);
    }

    // Write an SSF file to the FileSystem
    {
      OutputStream dataOut = null;
      OutputStream fileOut = fs.create(path);
      if (truncateSize >= 0) {
        fileOut = new UtilsForTests.TruncatedOutputStream(fileOut, truncateSize);
      }
      SimpleSeekableFormatCodec codec = ReflectionUtils.newInstance(SimpleSeekableFormatCodec.class, conf);
      dataOut = codec.createOutputStream(fileOut);

      // Try flush() with no data
      dataOut.flush();
      for (int r = 0; r < numRecords; r++) {
        if (flushRecords > 0 && r % flushRecords == 0) {
          dataOut.flush();
        }
        dataOut.write((getRecord(r) + "\n").getBytes("UTF-8"));
      }
      dataOut.close();
    }

    // Check file size
    FileStatus fileStatus = fs.getFileStatus(path);

    // Make TextInputFormat recognize .ssf via config
    JobConf job = new JobConf();
    job.set("fs.default.name", "file:///");
    // System.out.println("FS DEFAULT: " + job.get("fs.default.name"));
    job.set("io.compression.codecs", SimpleSeekableFormatCodec.class.getName());
    TextInputFormat textInputFormat = new TextInputFormat();
    textInputFormat.configure(job);

    // Open the file using TextInputFormat
    TextInputFormat.addInputPath(job, path);
    InputSplit[] splits = textInputFormat.getSplits(job, 1);
    Assert.assertEquals(1, splits.length);
    RecordReader<LongWritable, Text> recordReader = textInputFormat.getRecordReader(splits[0],
        job, Reporter.NULL);

    // Verify the data
    LongWritable key = recordReader.createKey();
    Text value = recordReader.createValue();
    int correctRecords = 0;
    boolean hitException = false;
    for (int r = 0; r < numRecords; r++) {
      boolean gotData = false;
      try {
        gotData = recordReader.next(key, value);
      } catch (CodecPrematureEOFException e) {
        // ignore this exception
        // gotData will be false and we will treat it as end of file.
        hitException = true;
        System.out.println("Hit CodecPrematureEOFException.");
      }
      if (truncateSize < 0) {
        // Do asserts only if the file is not truncated.
        Assert.assertTrue(gotData);
      }
      if (gotData) {
        Assert.assertEquals(getRecord(r), value.toString());
        correctRecords ++;
      } else {
        // Failed to get more records
        break;
      }
    }

    // Verify EOF
    if (!hitException) {
      boolean eof = !recordReader.next(key, value);
      Assert.assertTrue(eof);
    }
    recordReader.close();

    // Print stats
    System.out.println("DONE: File length=" + fileStatus.getLen() + " records=" + numRecords
        + " correctRecords=" + correctRecords + " truncateSize=" + truncateSize);

    // Clean up
    fs.delete(path);
  }

}
