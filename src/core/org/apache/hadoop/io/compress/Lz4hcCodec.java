/*
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

package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * This class creates lz4 compressors/decompressors.
 */
public class Lz4hcCodec extends Lz4Codec {

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public Compressor createCompressor() {
    if (!isNativeCodeLoaded()) {
      throw new RuntimeException("native lz4 library not available");
    }
    int bufferSize = conf.getInt(
        IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY,
        IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT);
    System.out.println("Create Lz4hc codec");
    return new Lz4Compressor(bufferSize, true);
  }
}
