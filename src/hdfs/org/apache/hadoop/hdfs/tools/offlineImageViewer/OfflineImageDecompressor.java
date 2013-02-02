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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.PositionTrackingInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSImageCompression;
import org.apache.hadoop.io.BufferedByteInputStream;

/**
 * OfflineImageViewer to dump the contents of an Hadoop image file to XML or the
 * console. Main entry point into utility, either via the command line or
 * programatically.
 */
public class OfflineImageDecompressor {

  private final static String usage = "Usage: bin/hdfs oid -i INPUTFILE -o OUTPUTFILE\n"
      + "Offline Image Decompressor\n"
      + "The oid utility will attempt to decompress image files.\n"
      + "The tool works offline and does not require a running cluster in\n"
      + "order to process an image file.\n"
      + "Required command line arguments:\n"
      + "-i,--inputFile <arg>   FSImage file to process.\n"
      + "-o,--outputFile <arg>  Name of output file. If the specified\n"
      + "                       file exists, it will be overwritten.\n";

  private final String inputFile;
  private final String outputFile;
  private int lastProgress = 0;

  public OfflineImageDecompressor(String inputFile, String outputFile) {
    this.inputFile = inputFile;
    this.outputFile = outputFile;
  }

  /**
   * Process image file.
   */
  private void go() throws IOException {
    long start = System.currentTimeMillis();
    System.out.println("Decompressing image file: " + inputFile + " to "
        + outputFile);
    DataInputStream in = null;
    DataOutputStream out = null;

    try {
      // setup in
      PositionTrackingInputStream ptis = new PositionTrackingInputStream(
          new FileInputStream(new File(inputFile)));
      in = new DataInputStream(ptis);

      // read header information
      int imgVersion = in.readInt();
      if (!LayoutVersion.supports(Feature.FSIMAGE_COMPRESSION, imgVersion)) {
        System.out
            .println("Image is not compressed. No output will be produced.");
        return;
      }
      int namespaceId = in.readInt();
      long numFiles = in.readLong();
      long genstamp = in.readLong();

      long imgTxId = -1;
      if (LayoutVersion.supports(Feature.STORED_TXIDS, imgVersion)) {
        imgTxId = in.readLong();
      }
      FSImageCompression compression = FSImageCompression
          .readCompressionHeader(new Configuration(), in);
      if (compression.isNoOpCompression()) {
        System.out
            .println("Image is not compressed. No output will be produced.");
        return;
      }
      in = BufferedByteInputStream.wrapInputStream(
          compression.unwrapInputStream(in), FSImage.LOAD_SAVE_BUFFER_SIZE,
          FSImage.LOAD_SAVE_CHUNK_SIZE);
      System.out.println("Starting decompression.");

      // setup output
      out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(
          outputFile)));

      // write back the uncompressed information
      out.writeInt(imgVersion);
      out.writeInt(namespaceId);
      out.writeLong(numFiles);
      out.writeLong(genstamp);
      if (LayoutVersion.supports(Feature.STORED_TXIDS, imgVersion)) {
        out.writeLong(imgTxId);
      }
      // no compression
      out.writeBoolean(false);

      // copy the data
      long size = new File(inputFile).length();
      // read in 1MB chunks
      byte[] block = new byte[1024 * 1024];
      while (true) {
        int bytesRead = in.read(block);
        if (bytesRead <= 0)
          break;
        out.write(block, 0, bytesRead);
        printProgress(ptis.getPos(), size);
      }

      out.close();

      long stop = System.currentTimeMillis();
      System.out.println("Input file : " + inputFile + " size: " + size);
      System.out.println("Output file: " + outputFile + " size: "
          + new File(outputFile).length());
      System.out.println("Decompression completed in " + (stop - start)
          + " ms.");
    } finally {
      if (in != null)
        in.close();
      if (out != null)
        out.close();
    }
  }

  /**
   * Print the progress.
   */
  private void printProgress(long read, long size) {
    int progress = Math.min(100, (int) ((100 * read) / size));
    if (progress > lastProgress) {
      lastProgress = progress;
      System.out.println("Completed " + lastProgress + " % ");
    }
  }

  /**
   * Build command-line options and descriptions
   */
  public static Options buildOptions() {
    Options options = new Options();

    // Build in/output file arguments, which are required, but there is no
    // addOption method that can specify this
    OptionBuilder.isRequired();
    OptionBuilder.hasArgs();
    OptionBuilder.withLongOpt("outputFile");
    options.addOption(OptionBuilder.create("o"));

    OptionBuilder.isRequired();
    OptionBuilder.hasArgs();
    OptionBuilder.withLongOpt("inputFile");
    options.addOption(OptionBuilder.create("i"));

    options.addOption("h", "help", false, "");
    return options;
  }

  /**
   * Entry point to command-line-driven operation. User may specify options and
   * start fsimage viewer from the command line. Program will process image file
   * and exit cleanly or, if an error is encountered, inform user and exit.
   * 
   * @param args
   *          Command line options
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Options options = buildOptions();
    if (args.length == 0) {
      printUsage();
      return;
    }

    CommandLineParser parser = new PosixParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("Error parsing command-line options: ");
      printUsage();
      return;
    }

    if (cmd.hasOption("h")) { // print help and exit
      printUsage();
      return;
    }

    String inputFile = cmd.getOptionValue("i");
    String outputFile = cmd.getOptionValue("o");

    try {
      OfflineImageDecompressor d = new OfflineImageDecompressor(inputFile,
          outputFile);
      d.go();
    } catch (EOFException e) {
      System.err.println("Input file ended unexpectedly.  Exiting");
      System.exit(255);
    } catch (IOException e) {
      System.err.println("Encountered exception.  Exiting: " + e.getMessage());
      System.exit(1);
    }
  }

  /**
   * Print application usage instructions.
   */
  private static void printUsage() {
    System.out.println(usage);
  }
}
