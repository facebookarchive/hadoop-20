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
package org.apache.hadoop.hdfs.protocol;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.io.CompatibleWritable;
import org.apache.hadoop.io.Writable;

import javax.validation.constraints.NotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdfs.protocol.ClientProtocol.GET_STATS_CAPACITY_IDX;
import static org.apache.hadoop.hdfs.protocol.ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX;
import static org.apache.hadoop.hdfs.protocol.ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX;
import static org.apache.hadoop.hdfs.protocol.ClientProtocol.GET_STATS_REMAINING_IDX;
import static org.apache.hadoop.hdfs.protocol.ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX;
import static org.apache.hadoop.hdfs.protocol.ClientProtocol.GET_STATS_USED_IDX;
import static org.apache.hadoop.hdfs.protocol.ClientProxyRequests.readStrings;
import static org.apache.hadoop.hdfs.protocol.ClientProxyRequests.writeStrings;

public class ClientProxyResponses {

  /** A wrapper around any Writable, allows wrapped value to be null */
  public static abstract class WrapperWritable<T extends Writable> implements Writable {
    private T value;

    public void set(T value) {
      this.value = value;
    }

    public T get() {
      return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      if (value == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        // We know that runtime type of serialized value representation is T
        create(value).write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      if (in.readBoolean()) {
        value = create();
        // We know that runtime type of value is T
        value.readFields(in);
      } else {
        value = null;
      }
    }

    /** Returns a new object of runtime type T */
    protected abstract T create();

    /**
     * Returns an object whose runtime type is T and contains information stored in the argument.
     * Despite misleading name there is no need to create a new object.
     * Example: let B extend A, B contains additional field which is serialized therefore we
     * cannot serialize A and deserialize B (or the other way around) without corrupting stream,
     * since static type that should be sent on wire is A we have to convert B to A (runtime types)
     * before serializing.
     * Note: if A is a final class then we know that there is no such B,
     * as a consequence we are safe to return passed object (we have V == T == object.getClass()).
     */
    protected <V extends T> T create(@NotNull V object) {
      return object;
    }
  }

  ////////////////////////////////////////
  // Wrapping responses
  ////////////////////////////////////////

  @ThriftStruct
  public static class BlockLocationsResponse extends WrapperWritable<LocatedBlocks> {
    @Override
    protected LocatedBlocks create() {
      return new LocatedBlocks();
    }

    @Override
    protected <V extends LocatedBlocks> LocatedBlocks create(@NotNull V object) {
      return new LocatedBlocks(object);
    }

    @SuppressWarnings("unused")
    public BlockLocationsResponse() {
    }

    @ThriftConstructor
    public BlockLocationsResponse(@ThriftField(1) LocatedBlocks blocks) {
      set(blocks);
    }

    @ThriftField(1)
    public LocatedBlocks getBlocks() {
      return get();
    }
  }

  @ThriftStruct
  public static class OpenResponse extends WrapperWritable<LocatedBlocksWithMetaInfo> {
    @Override
    protected LocatedBlocksWithMetaInfo create() {
      return new LocatedBlocksWithMetaInfo();
    }
    // Wrapped class is final, no create(V object) needed

    @SuppressWarnings("unused")
    public OpenResponse() {
    }

    @ThriftConstructor
    public OpenResponse(@ThriftField(1) LocatedBlocksWithMetaInfo blocks) {
      set(blocks);
    }

    @ThriftField(1)
    public LocatedBlocksWithMetaInfo getBlocks() {
      return get();
    }
  }

  @ThriftStruct
  public static class AppendResponse extends WrapperWritable<LocatedBlockWithOldGS> {
    @Override
    protected LocatedBlockWithOldGS create() {
      return new LocatedBlockWithOldGS();
    }
    // Wrapped class is final, no create(V object) needed

    @SuppressWarnings("unused")
    public AppendResponse() {
    }

    @ThriftConstructor
    public AppendResponse(@ThriftField(1) LocatedBlockWithOldGS block) {
      set(block);
    }

    @ThriftField(1)
    public LocatedBlockWithOldGS getBlock() {
      return get();
    }
  }

  @ThriftStruct
  public static class AddBlockResponse extends WrapperWritable<LocatedBlockWithMetaInfo> {
    @Override
    protected LocatedBlockWithMetaInfo create() {
      return new LocatedBlockWithMetaInfo();
    }

    @Override
    protected <V extends LocatedBlockWithMetaInfo> LocatedBlockWithMetaInfo create(
        @NotNull V object) {
      return new LocatedBlockWithMetaInfo(object);
    }

    @SuppressWarnings("unused")
    public AddBlockResponse() {
    }

    @ThriftConstructor
    public AddBlockResponse(@ThriftField(1) LocatedBlockWithMetaInfo block) {
      set(block);
    }

    @ThriftField(1)
    public LocatedBlockWithMetaInfo getBlock() {
      return get();
    }
  }

  @ThriftStruct
  public static class PartialListingResponse extends WrapperWritable<DirectoryListing> {
    @Override
    protected DirectoryListing create() {
      return new DirectoryListing();
    }

    @Override
    protected <V extends DirectoryListing> DirectoryListing create(@NotNull V object) {
      return new DirectoryListing(object);
    }

    @SuppressWarnings("unused")
    public PartialListingResponse() {
    }

    @ThriftConstructor
    public PartialListingResponse(@ThriftField(1) DirectoryListing listing) {
      set(listing);
    }

    @ThriftField(1)
    public DirectoryListing getListing() {
      return get();
    }
  }

  @ThriftStruct
  public static class LocatedPartialListingResponse
      extends WrapperWritable<LocatedDirectoryListing> {
    @Override
    protected LocatedDirectoryListing create() {
      return new LocatedDirectoryListing();
    }
    // Wrapped class is final, no create(V object) needed

    @SuppressWarnings("unused")
    public LocatedPartialListingResponse() {
    }

    @ThriftConstructor
    public LocatedPartialListingResponse(@ThriftField(1) LocatedDirectoryListing listing) {
      set(listing);
    }

    @ThriftField(1)
    public LocatedDirectoryListing getListing() {
      return get();
    }
  }

  @ThriftStruct
  public static class CorruptFileBlocksResponse extends WrapperWritable<CorruptFileBlocks> {
    @Override
    protected CorruptFileBlocks create() {
      return new CorruptFileBlocks();
    }
    // Wrapped class is final, no create(V object) needed

    @SuppressWarnings("unused")
    public CorruptFileBlocksResponse() {
    }

    @ThriftConstructor
    public CorruptFileBlocksResponse(@ThriftField(1) CorruptFileBlocks blocks) {
      set(blocks);
    }

    @ThriftField(1)
    public CorruptFileBlocks getBlocks() {
      return get();
    }
  }

  @ThriftStruct
  public static class FileInfoResponse extends WrapperWritable<HdfsFileStatus> {
    @Override
    protected HdfsFileStatus create() {
      return new HdfsFileStatus();
    }
    // Wrapped class is final, no create(V object) needed

    @SuppressWarnings("unused")
    public FileInfoResponse() {
    }

    @ThriftConstructor
    public FileInfoResponse(@ThriftField(1) HdfsFileStatus status) {
      set(status);
    }

    @ThriftField(1)
    public HdfsFileStatus getStatus() {
      return get();
    }
  }

  @ThriftStruct
  public static class ContentSummaryResponse extends WrapperWritable<ContentSummary> {
    @Override
    protected ContentSummary create() {
      return new ContentSummary();
    }
    // Wrapped class is final, no create(V object) needed

    @SuppressWarnings("unused")
    public ContentSummaryResponse() {
    }

    @ThriftConstructor
    public ContentSummaryResponse(@ThriftField(1) ContentSummary summary) {
      set(summary);
    }

    @ThriftField(1)
    public ContentSummary getSummary() {
      return get();
    }
  }

  @ThriftStruct
  public static class BlockInfoResponse extends WrapperWritable<LocatedBlockWithFileName> {
    @Override
    protected LocatedBlockWithFileName create() {
      return new LocatedBlockWithFileName();
    }
    // Wrapped class is final, no create(V object) needed

    @SuppressWarnings("unused")
    public BlockInfoResponse() {
    }

    @ThriftConstructor
    public BlockInfoResponse(@ThriftField(1) LocatedBlockWithFileName block) {
      set(block);
    }

    @ThriftField(1)
    public LocatedBlockWithFileName getBlock() {
      return get();
    }
  }

  ////////////////////////////////////////
  // Aggregate responses
  ////////////////////////////////////////

  @ThriftStruct
  public static class StatsResponse extends CompatibleWritable {
    /** Estimated size of structure on wire */
    private static final int SERIALIZED_SIZE = 2 * Long.SIZE * (new StatsResponse()
        .asArray().length);

    @ThriftField(1)
    public long capacity;
    @ThriftField(2)
    public long used;
    @ThriftField(3)
    public long remaining;
    @ThriftField(4)
    public long underReplicatedBlocks;
    @ThriftField(5)
    public long corruptBlocks;
    @ThriftField(6)
    public long missingBlocks;
    @ThriftField(7)
    public long usedNS;

    @SuppressWarnings("unused")
    public StatsResponse() {
    }

    public StatsResponse(long capacity, long used, long remaining, long underReplicatedBlocks,
        long corruptBlocks, long missingBlocks, long usedNS) {
      this.capacity = capacity;
      this.used = used;
      this.remaining = remaining;
      this.underReplicatedBlocks = underReplicatedBlocks;
      this.corruptBlocks = corruptBlocks;
      this.missingBlocks = missingBlocks;
      this.usedNS = usedNS;
    }

    public StatsResponse(long[] stats) {
      capacity = stats[GET_STATS_CAPACITY_IDX];
      used = stats[GET_STATS_USED_IDX];
      remaining = stats[GET_STATS_REMAINING_IDX];
      underReplicatedBlocks = stats[GET_STATS_UNDER_REPLICATED_IDX];
      corruptBlocks = stats[GET_STATS_CORRUPT_BLOCKS_IDX];
      missingBlocks = stats[GET_STATS_MISSING_BLOCKS_IDX];
      if (stats.length > 5) {
        usedNS = stats[6];
      }
    }

    @Override
    protected int getSizeEstimate() {
      return SERIALIZED_SIZE;
    }

    @Override
    protected byte[] getBuffer(int size) {
      return new byte[size];
    }

    @Override
    protected void freeBuffer(byte[] buffer) {
      // We do not cache buffers for this class
    }

    @Override
    public void writeCompatible(DataOutput out) throws IOException {
      out.writeLong(capacity);
      out.writeLong(used);
      out.writeLong(remaining);
      out.writeLong(underReplicatedBlocks);
      out.writeLong(corruptBlocks);
      out.writeLong(missingBlocks);
      out.writeLong(usedNS);
    }

    @Override
    public void readCompatible(CompatibleDataInput in) throws IOException {
      capacity = in.readLong();
      used = in.readLong();
      remaining = in.readLong();
      underReplicatedBlocks = in.readLong();
      corruptBlocks = in.readLong();
      missingBlocks = in.readLong();
      usedNS = in.readLong();
    }

    public long[] asArray() {
      return new long[]{capacity, used, remaining, underReplicatedBlocks, corruptBlocks,
          missingBlocks, usedNS};
    }
  }

  @ThriftStruct
  public static class HardLinkedFilesResponse implements Writable {
    private List<String> paths;

    @SuppressWarnings("unused")
    public HardLinkedFilesResponse() {
    }

    @ThriftConstructor
    public HardLinkedFilesResponse(@ThriftField(1) List<String> paths) {
      this.paths = paths;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      writeStrings(out, paths);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      paths = readStrings(in);
    }

    @ThriftField(1)
    public List<String> getPaths() {
      return paths;
    }
  }

  @ThriftStruct
  public static class IterativeGetOpenFilesResponse implements Writable {
    private List<OpenFileInfo> files;

    @SuppressWarnings("unused")
    public IterativeGetOpenFilesResponse() {
    }

    @ThriftConstructor
    public IterativeGetOpenFilesResponse(@ThriftField(1) List<OpenFileInfo> files) {
      this.files = files;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      if (files != null) {
        out.writeInt(files.size());
        for (OpenFileInfo elem : files) {
          OpenFileInfo.write(out, elem);
        }
      } else {
        out.writeInt(-1);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      final int fileSize = in.readInt();
      files = (fileSize < 0) ? null : new ArrayList<OpenFileInfo>(fileSize);
      for (int i = 0; i < fileSize; i++) {
        files.add(OpenFileInfo.read(in));
      }
    }

    @ThriftField(1)
    public List<OpenFileInfo> getFiles() {
      return files;
    }
  }

  @ThriftStruct
  public static class PingResponse implements Writable {
    private long response;

    @SuppressWarnings("unused")
    public PingResponse() {
    }

    @ThriftConstructor
    public PingResponse(@ThriftField(1) long response) {
      this.response = response;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(response);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      response = in.readLong();
    }

    @ThriftField(1)
    public long getResponse() {
      return response;
    }
  }
}
