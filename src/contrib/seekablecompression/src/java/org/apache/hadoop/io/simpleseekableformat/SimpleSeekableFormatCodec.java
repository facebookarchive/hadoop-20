package org.apache.hadoop.io.simpleseekableformat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Codec class for the SimpleSeekableFormat.
 *
 * Note that SimpleSeekableFormat is really a file format.  We are pretending it's a
 * codec to utilize Hadoop's file extension to codec mapping.
 *
 * As a result, we don't support Compressor, Decompressor, etc.
 * {@link org.apache.hadoop.io.simpleseekableformat.TestSimpleSeekableFormatCodec#testTextInputFormat()}
 * shows that it's not a problem for file extension to codec mapping.
 *
 * See {@link SimpleSeekableFormat}
 */
public class SimpleSeekableFormatCodec extends Configured implements CompressionCodec {

  @Override
  public Compressor createCompressor() {
    return null;
  }

  @Override
  public Decompressor createDecompressor() {
    return null;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return new SimpleSeekableFormatInputStream(in);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    return createInputStream(in);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    SimpleSeekableFormatOutputStream s = new SimpleSeekableFormatOutputStream(out);
    s.setConf(getConf());
    return s;
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    return createOutputStream(out);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return null;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return null;
  }

  @Override
  public String getDefaultExtension() {
    return ".ssf";
  }

}
