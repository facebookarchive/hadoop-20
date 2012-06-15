package org.apache.hadoop.io.simpleseekableformat;

import java.io.IOException;

/**
 * This exception is thrown when we detect corrupted data.
 */
public class CorruptedDataException extends IOException {

  private static final long serialVersionUID = 1L;

  public CorruptedDataException(String message) {
    super(message);
  }

  public CorruptedDataException(String message, Throwable cause) {
    super(message, cause);
  }

}
