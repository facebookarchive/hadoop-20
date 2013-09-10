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
package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class CompatibleWritable implements Writable {
  /** Initial version number */
  public static final int INITIAL_VERSION = 0;
  /** Default size of buffer which we use as a temporary storage for serialization */
  private static final int SERIALIZER_BUFFER_SIZE = 1024;
  /**
   * Encoded length of a message if it's initial version. For initial version we don't need to
   * write actual length, since the reader cannot be older than initial version.
   */
  private static final int INITIAL_VERSION_LENGTH = -1;

  private static final ThreadLocal<byte[]> serializerBuffers = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[SERIALIZER_BUFFER_SIZE];
    }
  };

  /**
   * Returns current version of an object, initial version is {@link #INITIAL_VERSION} (this is
   * important for optimizations). If we implement new, extended object (with new fields),
   * we have to increment version (that means add EXACTLY one).
   */
  protected int getVersion() {
    return INITIAL_VERSION;
  }

  /**
   * Returns good estimate (in the perfect scenario a tight upper bound) on the size of
   * serialized structure. Default implementation ensures that default sized buffer can be
   * retrieved from cache.
   */
  protected int getSizeEstimate() {
    return SERIALIZER_BUFFER_SIZE;
  }

  /**
   * Returns temporary buffer for serialization, note that if your {@link CompatibleWritable}
   * contains another {@link CompatibleWritable} you have to ensure that returned buffers are
   * unique. Default implementation is safe (maintains above property).
   */
  protected byte[] getBuffer(int size) {
    byte[] buffer;
    if (size > SERIALIZER_BUFFER_SIZE) {
      buffer = new byte[size];
    } else {
      buffer = serializerBuffers.get();
      serializerBuffers.remove();
    }
    return buffer;
  }

  /** Frees acquired buffer. */
  protected void freeBuffer(byte[] buffer) {
    if (buffer.length <= SERIALIZER_BUFFER_SIZE) {
      serializerBuffers.set(buffer);
    }
  }

  /**
   * Actual implementation of {@link Writable#write(DataOutput)} for this type.
   * Must call super#writeCompatible(DataOutputBuffer).
   */
  protected abstract void writeCompatible(DataOutput out) throws IOException;

  /**
   * Actual implementation of {@link Writable#readFields(DataInput)} for this type,
   * must check if {@link DataInputBuffer#canReadNextVersion()} before reading to ensure
   * compatibility.
   * Must call super#readCompatible(DataInputBuffer).
   */
  protected abstract void readCompatible(CompatibleDataInput in) throws IOException;

  @Override
  public final void write(DataOutput realOut) throws IOException {
    final int localVersion = getVersion();
    WritableUtils.writeVInt(realOut, localVersion);
    if (localVersion == INITIAL_VERSION) {
      WritableUtils.writeVInt(realOut, INITIAL_VERSION_LENGTH);
      writeCompatible(realOut);
    } else {
      final byte[] buffer = getBuffer(getSizeEstimate());
      try {
        DataOutputBuffer out = new DataOutputBuffer(buffer);
        // Write object
        writeCompatible(out);
        WritableUtils.writeVInt(realOut, out.getLength());
        realOut.write(out.getData(), 0, out.getLength());
      } finally {
        freeBuffer(buffer);
      }
    }
  }

  @Override
  public final void readFields(DataInput realIn) throws IOException {
    final int localVersion = getVersion();
    final int remoteVersion = WritableUtils.readVInt(realIn);
    if (remoteVersion <= localVersion) {
      // Discard length
      WritableUtils.readVInt(realIn);
      CompatibleDataInput in = new CompatibleDataInputWrapper(realIn, remoteVersion);
      readCompatible(in);
    } else {
      final int length = WritableUtils.readVInt(realIn);
      final byte[] buffer = getBuffer(length);
      try {
        realIn.readFully(buffer, 0, length);
        DataInputBuffer in = new DataInputBuffer();
        in.reset(buffer, length);
        // Read object
        readCompatible(in);
      } finally {
        freeBuffer(buffer);
      }
    }
  }

  /**
   * Provides calls to read data and check for possible compatibility problems.
   * Used to wrap {@link DataInput} in the case when the reader version is newer than the writer
   * version for object deserialization. This class provides information when the deserialization
   * needs to be stopped in such case, and the default values need to be assigned for the fields
   * that could not be deserialized.
   */
  public static interface CompatibleDataInput extends DataInput {
    /**
     * This function needs to be used when the reader is newer that the writer.
     * The reader reads fields, and for every version increment that occurred between the writer
     * and the reader, the reader needs to call this function to see if there is more data
     * available to be deserialized.
     * Please see {@link TestCompatibleWritable} for example.
     */
    boolean canReadNextVersion();
  }

  private static class CompatibleDataInputWrapper implements CompatibleDataInput {
    private final DataInput input;
    private final int serializedObjectVersion;
    private int currentVersion;

    public CompatibleDataInputWrapper(DataInput input, int serializedObjectVersion) {
      this.input = input;
      this.serializedObjectVersion = serializedObjectVersion;
      this.currentVersion = INITIAL_VERSION;
    }

    @Override
    public boolean canReadNextVersion() {
      if (currentVersion < serializedObjectVersion) {
        currentVersion++;
        return true;
      } else {
        return false;
      }
    }

    ////////////////////////////////////////
    // DataInput
    ////////////////////////////////////////

    @Override
    public void readFully(byte[] bytes) throws IOException {
      input.readFully(bytes);
    }

    @Override
    public void readFully(byte[] bytes, int i, int i2) throws IOException {
      input.readFully(bytes, i, i2);
    }

    @Override
    public int skipBytes(int i) throws IOException {
      return input.skipBytes(i);
    }

    @Override
    public boolean readBoolean() throws IOException {
      return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
      return input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
      return input.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
      return input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
      return input.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
      return input.readChar();
    }

    @Override
    public int readInt() throws IOException {
      return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
      return input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
      return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
      return input.readDouble();
    }

    @Override
    public String readLine() throws IOException {
      return input.readLine();
    }

    @Override
    public String readUTF() throws IOException {
      return input.readUTF();
    }
  }
}
