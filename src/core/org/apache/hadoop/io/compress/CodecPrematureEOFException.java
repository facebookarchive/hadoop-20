package org.apache.hadoop.io.compress;

import java.io.EOFException;

/**
 * Signals that end of stream has been reached unexpectedly while decompression.
 * 
 * This exception is mainly used by decompression data input streams to signal
 * that the codec was expecting more data from the underlying raw stream.
 * This usually happens when trying to read truncated compressed file.
 */
public class CodecPrematureEOFException extends EOFException {
  private static final long serialVersionUID = 2037410157019669762L;

  public CodecPrematureEOFException() {
    super();
  }
  
  public CodecPrematureEOFException(String message) {
    super(message);
  }
}
