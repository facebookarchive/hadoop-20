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
import java.io.OutputStream;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.lzma.*;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * A {@link org.apache.hadoop.io.compress.CompressionCodec} for a streaming
 * <b>lzma</b> compression/decompression pair.
 * http://www.7zip.org/
 * 
 */
public class LzmaCodec implements Configurable, CompressionCodec {
  
  private static final Log LOG = LogFactory.getLog(LzmaCodec.class.getName());

  private Configuration conf;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  public Configuration getConf() {
    return conf;
  }

  private static boolean nativeLzmaLoaded = false;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      nativeLzmaLoaded = LzmaCompressor.isNativeLzmaLoaded() &&
        LzmaDecompressor.isNativeLzmaLoaded();
      
      if (nativeLzmaLoaded) {
        LOG.info("Successfully loaded & initialized native-lzma library");
      } else {
        LOG.error("Failed to load/initialize native-lzma library");
      }
    } else {
      LOG.error("Cannot load native-lzma without native-hadoop");
    }
  }

  /**
   * Check if native-lzma library is loaded & initialized.
   * 
   * @param conf configuration
   * @return <code>true</code> if native-lzma library is loaded & initialized;
   *         else <code>false</code>
   */
  public static boolean isNativeLzmaLoaded(Configuration conf) {
    return nativeLzmaLoaded && conf.getBoolean("hadoop.native.lib", true);
  }

  public CompressionOutputStream createOutputStream(OutputStream out) 
    throws IOException {
    return createOutputStream(out, createCompressor());
  }
  
  public CompressionOutputStream createOutputStream(OutputStream out, 
      Compressor compressor) throws IOException {
    // Ensure native-lzma library is loaded & initialized
    if (!isNativeLzmaLoaded(conf)) {
      throw new RuntimeException("native-lzma library not available");
    }  
	
    int bufferSize =
      conf.getInt("io.compression.codec.lzma.buffersize", 64*1024);

//    return new BlockCompressorStream(out, compressor, bufferSize,
//                                     0);
    return new CompressorStream(out, compressor, bufferSize);
  }

  public Class<? extends Compressor> getCompressorType() {
    // Ensure native-lzma library is loaded & initialized
    if (!isNativeLzmaLoaded(conf)) {
      throw new RuntimeException("native-lzma library not available");
    }
    return LzmaCompressor.class;
  }

  public Compressor createCompressor() {
    // Ensure native-lzma library is loaded & initialized
    if (!isNativeLzmaLoaded(conf)) {
      throw new RuntimeException("native-lzma library not available");
    }
    
    int compressLevel =
        conf.getInt("io.compression.codec.lzma.compresslevel", 7);

    int bufferSize =
      conf.getInt("io.compression.codec.lzma.buffersize", 64*1024);

    return new LzmaCompressor(compressLevel, bufferSize);
  }

  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  public CompressionInputStream createInputStream(InputStream in, 
                                                  Decompressor decompressor) 
  throws IOException {
    // Ensure native-lzma library is loaded & initialized
    if (!isNativeLzmaLoaded(conf)) {
      throw new RuntimeException("native-lzma library not available");
    }
    //return new BlockDecompressorStream(in, decompressor, 
    //    conf.getInt("io.compression.codec.lzma.buffersize", 64*1024));

    return new DecompressorStream(in, decompressor, 
        conf.getInt("io.compression.codec.lzma.buffersize", 64*1024));
  }

  public Class<? extends Decompressor> getDecompressorType() {
    // Ensure native-lzma library is loaded & initialized
    if (!isNativeLzmaLoaded(conf)) {
      throw new RuntimeException("native-lzma library not available");
    }
    return LzmaDecompressor.class;
  }

  public Decompressor createDecompressor() {
    // Ensure native-lzma library is loaded & initialized
    if (!isNativeLzmaLoaded(conf)) {
      throw new RuntimeException("native-lzma library not available");
    }
    
    int bufferSize =
      conf.getInt("io.compression.codec.lzma.buffersize", 64*1024);

    return new LzmaDecompressor(bufferSize); 
  }

  /**
   * Get the default filename extension for this kind of compression.
   * @return the extension including the '.'
   */
  public String getDefaultExtension() {
    return ".lzma";
  }
}
