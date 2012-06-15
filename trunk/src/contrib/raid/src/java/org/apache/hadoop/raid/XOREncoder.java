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

package org.apache.hadoop.raid;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class XOREncoder extends Encoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.XOREncoder");
  public XOREncoder(
    Configuration conf) {
    super(conf, Codec.getCodec("xor"));
  }

  protected void encodeStripeImpl(
    InputStream[] blocks,
    long stripeStartOffset,
    long blockSize,
    OutputStream[] outs,
    Progressable reporter) throws IOException {
    int boundedBufferCapacity = 1;
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, blocks, bufSize, parallelism, boundedBufferCapacity, blockSize);
    parallelReader.start();
    try {
      encodeStripeParallel(blocks, stripeStartOffset, blockSize, outs,
        reporter, parallelReader);
    } finally {
      parallelReader.shutdown();
    }
  }

  protected void encodeStripeParallel(
    InputStream[] blocks,
    long stripeStartOffset,
    long blockSize,
    OutputStream[] outs,
    Progressable reporter,
    ParallelStreamReader parallelReader) throws IOException {
    ParallelStreamReader.ReadResult readResult;
    for (long encoded = 0; encoded < blockSize; encoded += bufSize) {
      try {
        readResult = parallelReader.getReadResult();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for read result");
      }
      // Cannot tolerate any IO errors.
      IOException readEx = readResult.getException();
      if (readEx != null) {
        throw readEx;
      }

      xor(readResult.readBufs, writeBufs[0]);
      reporter.progress();

      // Write to output
      outs[0].write(writeBufs[0], 0, bufSize);
      reporter.progress();
    }
  }

  static void xor(byte[][] inputs, byte[] output) {
    int bufSize = output.length;
    // Get the first buffer's data.
    for (int j = 0; j < bufSize; j++) {
      output[j] = inputs[0][j];
    }
    // XOR with everything else.
    for (int i = 1; i < inputs.length; i++) {
      for (int j = 0; j < bufSize; j++) {
        output[j] ^= inputs[i][j];
      }
    }
  }

}
