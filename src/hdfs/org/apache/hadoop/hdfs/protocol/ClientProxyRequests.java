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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.*;
import org.apache.hadoop.io.CompatibleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;

import javax.validation.constraints.NotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Every call in {@link ClientProxyProtocol} is bijectively associated with a single object which
 * extends {@link Request} and carries all of its arguments, type of this object denotes which
 * call it belongs to.
 */
@SuppressWarnings("override")
public final class ClientProxyRequests {
  private ClientProxyRequests() {
  }

  ///////////////////////////////////////
  // Generic requests
  ///////////////////////////////////////

  /** Carries information about call other than method name and arguments */
  @ThriftStruct
  public static final class RequestMetaInfo extends CompatibleWritable {
    /** Expected upper bound on the size of serialized {@link RequestMetaInfo} */
    private static final int SERIALIZER_BUFFER_SIZE = 512;

    private static final ThreadLocal<byte[]> serializerBuffers = new ThreadLocal<byte[]>() {
      @Override
      protected byte[] initialValue() {
        return new byte[SERIALIZER_BUFFER_SIZE];
      }
    };

    // We assume that each of the following fields has an unique id for efficient handling of
    // optional fields (presence of each field is indicated by appropriate bit in the bitmask
    // that is sent over the wire).

    public static final int NO_CLUSTER_ID = -1;
    private int clusterId; // field id = 1

    private String resourceId; // field id = 2

    public static final int NO_NAMESPACE_ID = -1;
    private int namespaceId; // field id = 3

    public static final int NO_APPLICATION_ID = -1;
    private int applicationId; // field id = 4

    private UnixUserGroupInformation origCaller; // field id = 5

    public RequestMetaInfo() {
      this(NO_CLUSTER_ID, null, NO_NAMESPACE_ID, NO_APPLICATION_ID, null);
    }

    @ThriftConstructor
    public RequestMetaInfo(int clusterId, String resourceId, int namespaceId, int applicationId,
        UnixUserGroupInformation origCaller) {
      this.clusterId = clusterId;
      this.resourceId = resourceId;
      this.namespaceId = namespaceId;
      this.applicationId = applicationId;
      this.origCaller = origCaller;
    }

    @Override
    protected int getSizeEstimate() {
      return SERIALIZER_BUFFER_SIZE;
    }

    @Override
    protected byte[] getBuffer(int size) {
      byte[] buffer;
      if (size > SERIALIZER_BUFFER_SIZE) {
        buffer = new byte[size];
      } else {
        buffer = serializerBuffers.get();
        // We would normally remove buffer but RequestMetaInfo does not contain itself so we cannot
        // ask for a buffer twice without returning it in between.
      }
      return buffer;
    }

    @Override
    protected void freeBuffer(byte[] buffer) {
      // Never removed buffer, no need to return it
    }

    @Override
    protected void writeCompatible(DataOutput out) throws IOException {
      // Compute and write field presence mask
      int issetMask = 0;
      issetMask |= (clusterId == NO_CLUSTER_ID) ? 0 : getFieldPresenceMask(1);
      issetMask |= (resourceId == null) ? 0 : getFieldPresenceMask(2);
      issetMask |= (namespaceId == NO_NAMESPACE_ID) ? 0 : getFieldPresenceMask(3);
      issetMask |= (applicationId == NO_APPLICATION_ID) ? 0 : getFieldPresenceMask(4);
      issetMask |= (origCaller == null) ? 0 : getFieldPresenceMask(5);
      WritableUtils.writeVInt(out, issetMask);
      // Serialize fields if present
      if (isFieldPresent(issetMask, 1)) WritableUtils.writeVInt(out, clusterId);
      if (isFieldPresent(issetMask, 2)) Text.writeStringOpt(out, resourceId);
      if (isFieldPresent(issetMask, 3)) WritableUtils.writeVInt(out, namespaceId);
      if (isFieldPresent(issetMask, 4)) WritableUtils.writeVInt(out, applicationId);
      if (isFieldPresent(issetMask, 5)) origCaller.write(out);
    }

    @Override
    protected void readCompatible(CompatibleDataInput in) throws IOException {
      int issetMask = WritableUtils.readVInt(in);
      // Deserialize fields if present
      clusterId = isFieldPresent(issetMask, 1) ? WritableUtils.readVInt(in) : NO_CLUSTER_ID;
      resourceId = isFieldPresent(issetMask, 2) ? Text.readStringOpt(in) : null;
      namespaceId = isFieldPresent(issetMask, 3) ? WritableUtils.readVInt(in) : NO_NAMESPACE_ID;
      applicationId = isFieldPresent(issetMask, 4) ? WritableUtils.readVInt(in) : NO_APPLICATION_ID;
      if (isFieldPresent(issetMask, 5)) {
        origCaller = new UnixUserGroupInformation();
        origCaller.readFields(in);
      } else {
        origCaller = null;
      }
    }

    @ThriftField(1)
    public int getClusterId() {
      return clusterId;
    }

    @ThriftField(2)
    public String getResourceId() {
      return resourceId;
    }

    @ThriftField(3)
    public int getNamespaceId() {
      return namespaceId;
    }

    @ThriftField(4)
    public int getApplicationId() {
      return applicationId;
    }

    @ThriftField(5)
    public UnixUserGroupInformation getOrigCaller() {
      return origCaller;
    }

    private int getFieldPresenceMask(int fieldId) {
      return 1 << (fieldId - 1);
    }

    private boolean isFieldPresent(int mask, int fieldId) {
      return (mask & getFieldPresenceMask(fieldId)) != 0;
    }
  }

  /** Describes an empty call issued through ClientProxyProtocol */
  public static abstract class Request<T> implements Writable {
    private RequestMetaInfo requestMetaInfo;

    protected Request() {
      this(new RequestMetaInfo());
    }

    protected Request(@NotNull RequestMetaInfo requestMetaInfo) {
      notNull(requestMetaInfo);
      this.requestMetaInfo = requestMetaInfo;
    }

    public RequestMetaInfo getRequestMetaInfo() {
      return requestMetaInfo;
    }

    public UnixUserGroupInformation getOriginalCaller() {
      return requestMetaInfo.getOrigCaller();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      requestMetaInfo.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      requestMetaInfo.readFields(in);
    }

    public abstract T call(ClientProxyProtocol namenode) throws IOException;
  }

  /** Describes a call which carries client name */
  private static abstract class ClientRequest<T> extends Request<T> {
    /** A name of requesting client */
    private String clientName;

    protected ClientRequest() {
    }

    protected ClientRequest(RequestMetaInfo requestMetaInfo, String clientName) {
      super(requestMetaInfo);
      this.clientName = clientName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeStringOpt(out, clientName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      clientName = Text.readStringOpt(in);
    }

    public String getClientName() {
      return clientName;
    }
  }

  /** Describes call which carries source path */
  private static abstract class SourceRequest<T> extends Request<T> {
    /** A source file path that this request refers to */
    private String src;

    protected SourceRequest() {
    }

    protected SourceRequest(RequestMetaInfo requestMetaInfo, String src) {
      super(requestMetaInfo);
      this.src = src;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeStringOpt(out, src);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      src = Text.readStringOpt(in);
    }

    public String getSrc() {
      return src;
    }
  }

  /** Describes call which carries source and destination path */
  private static abstract class SourceDestinationRequest<T> extends SourceRequest<T> {
    /** A destination file path that this request refers to */
    private String dst;

    protected SourceDestinationRequest() {
    }

    protected SourceDestinationRequest(RequestMetaInfo requestMetaInfo, String src, String dst) {
      super(requestMetaInfo, src);
      this.dst = dst;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeStringOpt(out, dst);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      dst = Text.readStringOpt(in);
    }

    public String getDst() {
      return dst;
    }
  }

  /** Describes a call which carries client name and source path */
  private static abstract class SourceAndClientRequest<T> extends SourceRequest<T> {
    /** A name of requesting client */
    private String clientName;

    protected SourceAndClientRequest() {
    }

    protected SourceAndClientRequest(RequestMetaInfo requestMetaInfo, String src,
        String clientName) {
      super(requestMetaInfo, src);
      this.clientName = clientName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeStringOpt(out, clientName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      clientName = Text.readStringOpt(in);
    }

    public String getClientName() {
      return clientName;
    }
  }

  ///////////////////////////////////////
  // Specific requests and responses
  ///////////////////////////////////////

  @ThriftStruct
  public static class GetBlockLocationsRequest extends SourceRequest<BlockLocationsResponse> {
    private long offset;
    private long length;

    @SuppressWarnings("unused")
    public GetBlockLocationsRequest() {
    }

    @ThriftConstructor
    public GetBlockLocationsRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) long offset, @ThriftField(4) long length) {
      super(requestMetaInfo, src);
      this.offset = offset;
      this.length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeLong(offset);
      out.writeLong(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      offset = in.readLong();
      length = in.readLong();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public long getOffset() {
      return offset;
    }

    @ThriftField(4)
    public long getLength() {
      return length;
    }

    @Override
    public BlockLocationsResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getBlockLocations(this);
    }
  }

  @ThriftStruct
  public static class OpenRequest extends SourceRequest<OpenResponse> {
    private long offset;
    private long length;

    @SuppressWarnings("unused")
    public OpenRequest() {
    }

    @ThriftConstructor
    public OpenRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo, @ThriftField(2) String src,
        @ThriftField(3) long offset, @ThriftField(4) long length) {
      super(requestMetaInfo, src);
      this.offset = offset;
      this.length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeLong(offset);
      out.writeLong(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      offset = in.readLong();
      length = in.readLong();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public long getOffset() {
      return offset;
    }

    @ThriftField(4)
    public long getLength() {
      return length;
    }

    @Override
    public OpenResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.open(this);
    }
  }

  @ThriftStruct
  public static class CreateRequest extends SourceAndClientRequest<Void> {
    private FsPermission masked = FsPermission.getDefault();
    private boolean overwrite;
    private boolean createParent;
    private short replication;
    private long blockSize;

    @SuppressWarnings("unused")
    public CreateRequest() {
    }

    @ThriftConstructor
    public CreateRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) String clientName,
        @ThriftField(4) @NotNull FsPermission masked, @ThriftField(5) boolean overwrite,
        @ThriftField(6) boolean createParent, @ThriftField(7) short replication,
        @ThriftField(8) long blockSize) {
      super(requestMetaInfo, src, clientName);
      notNull(masked);
      this.masked = masked;
      this.overwrite = overwrite;
      this.createParent = createParent;
      this.replication = replication;
      this.blockSize = blockSize;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      FsPermission.write(out, masked);
      out.writeBoolean(overwrite);
      out.writeBoolean(createParent);
      out.writeShort(replication);
      out.writeLong(blockSize);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      masked = FsPermission.read(in);
      overwrite = in.readBoolean();
      createParent = in.readBoolean();
      replication = in.readShort();
      blockSize = in.readLong();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getClientName() {
      return super.getClientName();
    }

    @ThriftField(4)
    public FsPermission getMasked() {
      return masked;
    }

    @ThriftField(5)
    public boolean isOverwrite() {
      return overwrite;
    }

    @ThriftField(6)
    public boolean isCreateParent() {
      return createParent;
    }

    @ThriftField(7)
    public short getReplication() {
      return replication;
    }

    @ThriftField(8)
    public long getBlockSize() {
      return blockSize;
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.create(this);
      return null;
    }
  }

  @ThriftStruct
  public static class AppendRequest extends SourceAndClientRequest<AppendResponse> {
    @SuppressWarnings("unused")
    public AppendRequest() {
    }

    @ThriftConstructor
    public AppendRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) String clientName) {
      super(requestMetaInfo, src, clientName);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getClientName() {
      return super.getClientName();
    }

    @Override
    public AppendResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.append(this);
    }
  }

  @ThriftStruct
  public static class RecoverLeaseRequest extends SourceAndClientRequest<Void> {
    @SuppressWarnings("unused")
    public RecoverLeaseRequest() {
    }

    @ThriftConstructor
    public RecoverLeaseRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) String clientName) {
      super(requestMetaInfo, src, clientName);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getClientName() {
      return super.getClientName();
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.recoverLease(this);
      return null;
    }
  }

  @ThriftStruct
  public static class CloseRecoverLeaseRequest extends SourceAndClientRequest<Boolean> {
    private boolean discardLastBlock;

    @SuppressWarnings("unused")
    public CloseRecoverLeaseRequest() {
    }

    @ThriftConstructor
    public CloseRecoverLeaseRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) String clientName,
        @ThriftField(4) boolean discardLastBlock) {
      super(requestMetaInfo, src, clientName);
      this.discardLastBlock = discardLastBlock;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeBoolean(discardLastBlock);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      discardLastBlock = in.readBoolean();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getClientName() {
      return super.getClientName();
    }

    @ThriftField(4)
    public boolean isDiscardLastBlock() {
      return discardLastBlock;
    }

    @Override
    public Boolean call(ClientProxyProtocol namenode) throws IOException {
      return namenode.closeRecoverLease(this);
    }
  }

  @ThriftStruct
  public static class SetReplicationRequest extends SourceRequest<Boolean> {
    private short replication;

    @SuppressWarnings("unused")
    public SetReplicationRequest() {
    }

    @ThriftConstructor
    public SetReplicationRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) short replication) {
      super(requestMetaInfo, src);
      this.replication = replication;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeShort(replication);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      replication = in.readShort();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public short getReplication() {
      return replication;
    }

    @Override
    public Boolean call(ClientProxyProtocol namenode) throws IOException {
      return namenode.setReplication(this);
    }
  }

  @ThriftStruct
  public static class SetPermissionRequest extends SourceRequest<Void> {
    private FsPermission permission = FsPermission.getDefault();

    @SuppressWarnings("unused")
    public SetPermissionRequest() {
    }

    @ThriftConstructor
    public SetPermissionRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) @NotNull FsPermission permission) {
      super(requestMetaInfo, src);
      notNull(permission);
      this.permission = permission;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      FsPermission.write(out, permission);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      permission = FsPermission.read(in);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public FsPermission getPermission() {
      return permission;
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.setPermission(this);
      return null;
    }
  }

  @ThriftStruct
  public static class SetOwnerRequest extends SourceRequest<Void> {
    private String username;
    private String groupname;

    @SuppressWarnings("unused")
    public SetOwnerRequest() {
    }

    @ThriftConstructor
    public SetOwnerRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) String username,
        @ThriftField(4) String groupname) {
      super(requestMetaInfo, src);
      this.username = username;
      this.groupname = groupname;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeStringOpt(out, username);
      Text.writeStringOpt(out, groupname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      username = Text.readStringOpt(in);
      groupname = Text.readStringOpt(in);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getUsername() {
      return username;
    }

    @ThriftField(4)
    public String getGroupname() {
      return groupname;
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.setOwner(this);
      return null;
    }
  }

  @ThriftStruct
  public static class AbandonBlockRequest extends AbandonFileRequest {
    private Block block;

    @SuppressWarnings("unused")
    public AbandonBlockRequest() {
    }

    @ThriftConstructor
    public AbandonBlockRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) String clientName,
        @ThriftField(4) Block block) {
      super(requestMetaInfo, src, clientName);
      this.block = block;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Block.writeSafe(out, block);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      block = Block.readSafe(in);
    }

    @ThriftField(4)
    public Block getBlock() {
      return block;
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.abandonBlock(this);
      return null;
    }
  }

  @ThriftStruct
  public static class AbandonFileRequest extends SourceAndClientRequest<Void> {
    public AbandonFileRequest() {
    }

    @ThriftConstructor
    public AbandonFileRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) String src, @ThriftField(3) String clientName) {
      super(requestMetaInfo, src, clientName);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getClientName() {
      return super.getClientName();
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.abandonFile(this);
      return null;
    }
  }

  @ThriftStruct
  public static class AddBlockRequest extends SourceAndClientRequest<AddBlockResponse> {
    private List<DatanodeInfo> excludedNodes;
    private List<DatanodeInfo> favoredNodes;
    private long startPos;
    private Block lastBlock;

    @SuppressWarnings("unused")
    public AddBlockRequest() {
    }

    @ThriftConstructor
    public AddBlockRequest(RequestMetaInfo requestMetaInfo, String src, String clientName,
        List<DatanodeInfo> excludedNodes, List<DatanodeInfo> favoredNodes, long startPos,
        Block lastBlock) {
      super(requestMetaInfo, src, clientName);
      this.excludedNodes = excludedNodes;
      this.favoredNodes = favoredNodes;
      this.startPos = startPos;
      this.lastBlock = lastBlock;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      DatanodeInfo.writeList(out, excludedNodes);
      DatanodeInfo.writeList(out, favoredNodes);
      out.writeLong(startPos);
      Block.writeSafe(out, lastBlock);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      excludedNodes = DatanodeInfo.readList(in);
      favoredNodes = DatanodeInfo.readList(in);
      startPos = in.readLong();
      lastBlock = Block.readSafe(in);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getClientName() {
      return super.getClientName();
    }

    @ThriftField(4)
    public List<DatanodeInfo> getExcludedNodes() {
      return excludedNodes;
    }

    @ThriftField(5)
    public List<DatanodeInfo> getFavoredNodes() {
      return favoredNodes;
    }

    @ThriftField(6)
    public long getStartPos() {
      return startPos;
    }

    @ThriftField(7)
    public Block getLastBlock() {
      return lastBlock;
    }

    @Override
    public AddBlockResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.addBlock(this);
    }
  }

  @ThriftStruct
  public static class CompleteRequest extends SourceAndClientRequest<Boolean> {
    private long fileLen;
    private Block lastBlock;

    @SuppressWarnings("unused")
    public CompleteRequest() {
    }

    @ThriftConstructor
    public CompleteRequest(RequestMetaInfo requestMetaInfo, String src, String clientName,
        long fileLen, Block lastBlock) {
      super(requestMetaInfo, src, clientName);
      this.fileLen = fileLen;
      this.lastBlock = lastBlock;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeLong(fileLen);
      Block.writeSafe(out, lastBlock);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      fileLen = in.readLong();
      lastBlock = Block.readSafe(in);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getClientName() {
      return super.getClientName();
    }

    @ThriftField(4)
    public long getFileLen() {
      return fileLen;
    }

    @ThriftField(5)
    public Block getLastBlock() {
      return lastBlock;
    }

    @Override
    public Boolean call(ClientProxyProtocol namenode) throws IOException {
      return namenode.complete(this);
    }
  }

  @ThriftStruct
  public static class ReportBadBlocksRequest extends Request<Void> {
    private List<LocatedBlock> blocks;

    @SuppressWarnings("unused")
    public ReportBadBlocksRequest() {
    }

    @ThriftConstructor
    public ReportBadBlocksRequest(RequestMetaInfo requestMetaInfo, List<LocatedBlock> blocks) {
      super(requestMetaInfo);
      this.blocks = blocks;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      if (blocks != null) {
        out.writeInt(blocks.size());
        for (LocatedBlock elem : blocks) {
          LocatedBlock.write(out, elem);
        }
      } else {
        out.writeInt(-1);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      final int blocksSize = in.readInt();
      blocks = (blocksSize < 0) ? null : new ArrayList<LocatedBlock>(blocksSize);
      for (int i = 0; i < blocksSize; i++) {
        blocks.add(LocatedBlock.read(in));
      }
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public List<LocatedBlock> getBlocks() {
      return blocks;
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.reportBadBlocks(this);
      return null;
    }
  }

  @ThriftStruct
  public static class HardLinkRequest extends SourceDestinationRequest<Boolean> {
    @SuppressWarnings("unused")
    public HardLinkRequest() {
    }

    @ThriftConstructor
    public HardLinkRequest(RequestMetaInfo requestMetaInfo, String src, String dst) {
      super(requestMetaInfo, src, dst);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getDst() {
      return super.getDst();
    }

    @Override
    public Boolean call(ClientProxyProtocol namenode) throws IOException {
      return namenode.hardLink(this);
    }
  }

  @ThriftStruct
  public static class GetHardLinkedFilesRequest extends SourceRequest<HardLinkedFilesResponse> {
    @SuppressWarnings("unused")
    public GetHardLinkedFilesRequest() {
    }

    @ThriftConstructor
    public GetHardLinkedFilesRequest(RequestMetaInfo requestMetaInfo, String src) {
      super(requestMetaInfo, src);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @Override
    public HardLinkedFilesResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getHardLinkedFiles(this);
    }
  }

  @ThriftStruct
  public static class RenameRequest extends SourceDestinationRequest<Boolean> {
    @SuppressWarnings("unused")
    public RenameRequest() {
    }

    @ThriftConstructor
    public RenameRequest(RequestMetaInfo requestMetaInfo, String src, String dst) {
      super(requestMetaInfo, src, dst);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getDst() {
      return super.getDst();
    }

    @Override
    public Boolean call(ClientProxyProtocol namenode) throws IOException {
      return namenode.rename(this);
    }
  }

  @ThriftStruct
  public static class ConcatRequest extends Request<Void> {
    private String trg;
    private List<String> srcs;
    private boolean restricted;

    @SuppressWarnings("unused")
    public ConcatRequest() {
    }

    @ThriftConstructor
    public ConcatRequest(RequestMetaInfo requestMetaInfo, String trg, List<String> srcs,
        boolean restricted) {
      super(requestMetaInfo);
      this.trg = trg;
      this.srcs = srcs;
      this.restricted = restricted;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeStringOpt(out, trg);
      if (srcs != null) {
        out.writeInt(srcs.size());
        for (String elem : srcs) {
          Text.writeStringOpt(out, elem);
        }
      } else {
        out.writeInt(-1);
      }
      out.writeBoolean(restricted);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      trg = Text.readStringOpt(in);
      final int srcsSize = in.readInt();
      srcs = (srcsSize < 0) ? null : new ArrayList<String>(srcsSize);
      for (int i = 0; i < srcsSize; i++) {
        srcs.add(Text.readStringOpt(in));
      }
      restricted = in.readBoolean();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getTrg() {
      return trg;
    }

    @ThriftField(3)
    public List<String> getSrcs() {
      return srcs;
    }

    @ThriftField(4)
    public boolean isRestricted() {
      return restricted;
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.concat(this);
      return null;
    }
  }

  @ThriftStruct
  public static class DeleteRequest extends SourceRequest<Boolean> {
    private boolean recursive;

    @SuppressWarnings("unused")
    public DeleteRequest() {
    }

    @ThriftConstructor
    public DeleteRequest(RequestMetaInfo requestMetaInfo, String src, boolean recursive) {
      super(requestMetaInfo, src);
      this.recursive = recursive;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeBoolean(recursive);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      recursive = in.readBoolean();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public boolean isRecursive() {
      return recursive;
    }

    @Override
    public Boolean call(ClientProxyProtocol namenode) throws IOException {
      return namenode.delete(this);
    }
  }

  @ThriftStruct
  public static class MkdirsRequest extends SourceRequest<Boolean> {
    private FsPermission masked = FsPermission.getDefault();

    @SuppressWarnings("unused")
    public MkdirsRequest() {
    }

    @ThriftConstructor
    public MkdirsRequest(RequestMetaInfo requestMetaInfo, String src,
        @NotNull FsPermission masked) {
      super(requestMetaInfo, src);
      notNull(masked);
      this.masked = masked;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      FsPermission.write(out, masked);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      masked = FsPermission.read(in);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public FsPermission getMasked() {
      return masked;
    }

    @Override
    public Boolean call(ClientProxyProtocol namenode) throws IOException {
      return namenode.mkdirs(this);
    }
  }

  @ThriftStruct
  public static class IterativeGetOpenFilesRequest
      extends SourceRequest<IterativeGetOpenFilesResponse> {
    private int millis;
    private String start;

    @SuppressWarnings("unused")
    public IterativeGetOpenFilesRequest() {
    }

    @ThriftConstructor
    public IterativeGetOpenFilesRequest(RequestMetaInfo requestMetaInfo, String src, int millis,
        String start) {
      super(requestMetaInfo, src);
      this.millis = millis;
      this.start = start;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(millis);
      Text.writeStringOpt(out, start);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      millis = in.readInt();
      start = Text.readStringOpt(in);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public int getMillis() {
      return millis;
    }

    @ThriftField(4)
    public String getStart() {
      return start;
    }

    @Override
    public IterativeGetOpenFilesResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.iterativeGetOpenFiles(this);
    }
  }

  @ThriftStruct
  public static class GetPartialListingRequest extends SourceRequest<PartialListingResponse> {
    private byte[] startAfter = new byte[0];

    @SuppressWarnings("unused")
    public GetPartialListingRequest() {
    }

    @ThriftConstructor
    public GetPartialListingRequest(RequestMetaInfo requestMetaInfo, String src,
        byte[] startAfter) {
      super(requestMetaInfo, src);
      this.startAfter = startAfter;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(startAfter.length);
      out.write(startAfter);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      startAfter = new byte[in.readInt()];
      in.readFully(startAfter);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public byte[] getStartAfter() {
      return startAfter;
    }

    @Override
    public PartialListingResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getPartialListing(this);
    }
  }

  @ThriftStruct
  public static class GetLocatedPartialListingRequest
      extends SourceRequest<LocatedPartialListingResponse> {
    private byte[] startAfter = new byte[0];

    @SuppressWarnings("unused")
    public GetLocatedPartialListingRequest() {
    }

    @ThriftConstructor
    public GetLocatedPartialListingRequest(RequestMetaInfo requestMetaInfo, String src,
        byte[] startAfter) {
      super(requestMetaInfo, src);
      this.startAfter = startAfter;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(startAfter.length);
      out.write(startAfter);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      startAfter = new byte[in.readInt()];
      in.readFully(startAfter);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public byte[] getStartAfter() {
      return startAfter;
    }

    @Override
    public LocatedPartialListingResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getLocatedPartialListing(this);
    }
  }

  @ThriftStruct
  public static class RenewLeaseRequest extends ClientRequest<Void> {
    @SuppressWarnings("unused")
    public RenewLeaseRequest() {
    }

    @ThriftConstructor
    public RenewLeaseRequest(RequestMetaInfo requestMetaInfo, String clientName) {
      super(requestMetaInfo, clientName);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getClientName() {
      return super.getClientName();
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.renewLease(this);
      return null;
    }
  }

  @ThriftStruct
  public static class GetStatsRequest extends Request<StatsResponse> {
    @SuppressWarnings("unused")
    public GetStatsRequest() {
    }

    @ThriftConstructor
    public GetStatsRequest(RequestMetaInfo requestMetaInfo) {
      super(requestMetaInfo);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @Override
    public StatsResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getStats(this);
    }
  }

  @ThriftStruct
  public static class GetPreferredBlockSizeRequest extends SourceRequest<Long> {
    @SuppressWarnings("unused")
    public GetPreferredBlockSizeRequest() {
    }

    @ThriftConstructor
    public GetPreferredBlockSizeRequest(RequestMetaInfo requestMetaInfo, String src) {
      super(requestMetaInfo, src);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @Override
    public Long call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getPreferredBlockSize(this);
    }
  }

  @ThriftStruct
  public static class ListCorruptFileBlocksRequest
      extends SourceRequest<CorruptFileBlocksResponse> {
    private String cookie;

    @SuppressWarnings("unused")
    public ListCorruptFileBlocksRequest() {
    }

    @ThriftConstructor
    public ListCorruptFileBlocksRequest(RequestMetaInfo requestMetaInfo, String src,
        String cookie) {
      super(requestMetaInfo, src);
      this.cookie = cookie;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeStringOpt(out, cookie);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      cookie = Text.readStringOpt(in);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getCookie() {
      return cookie;
    }

    @Override
    public CorruptFileBlocksResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.listCorruptFileBlocks(this);
    }
  }

  @ThriftStruct
  public static class GetFileInfoRequest extends SourceRequest<FileInfoResponse> {
    @SuppressWarnings("unused")
    public GetFileInfoRequest() {
    }

    @ThriftConstructor
    public GetFileInfoRequest(RequestMetaInfo requestMetaInfo, String src) {
      super(requestMetaInfo, src);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @Override
    public FileInfoResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getFileInfo(this);
    }
  }

  @ThriftStruct
  public static class GetContentSummaryRequest extends SourceRequest<ContentSummaryResponse> {
    @SuppressWarnings("unused")
    public GetContentSummaryRequest() {
    }

    @ThriftConstructor
    public GetContentSummaryRequest(RequestMetaInfo requestMetaInfo, String src) {
      super(requestMetaInfo, src);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @Override
    public ContentSummaryResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getContentSummary(this);
    }
  }

  @ThriftStruct
  public static class FSyncRequest extends SourceAndClientRequest<Void> {
    @SuppressWarnings("unused")
    public FSyncRequest() {
    }

    @ThriftConstructor
    public FSyncRequest(RequestMetaInfo requestMetaInfo, String src, String clientName) {
      super(requestMetaInfo, src, clientName);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getClientName() {
      return super.getClientName();
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.fsync(this);
      return null;
    }
  }

  @ThriftStruct
  public static class SetTimesRequest extends SourceRequest<Void> {
    private long mtime;
    private long atime;

    @SuppressWarnings("unused")
    public SetTimesRequest() {
    }

    @ThriftConstructor
    public SetTimesRequest(RequestMetaInfo requestMetaInfo, String src, long mtime, long atime) {
      super(requestMetaInfo, src);
      this.mtime = mtime;
      this.atime = atime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeLong(mtime);
      out.writeLong(atime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      mtime = in.readLong();
      atime = in.readLong();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public long getMtime() {
      return mtime;
    }

    @ThriftField(4)
    public long getAtime() {
      return atime;
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.setTimes(this);
      return null;
    }
  }

  @ThriftStruct
  public static class UpdatePipelineRequest extends ClientRequest<Void> {
    private Block oldBlock;
    private Block newBlock;
    private List<DatanodeID> newNodes;

    @SuppressWarnings("unused")
    public UpdatePipelineRequest() {
    }

    @ThriftConstructor
    public UpdatePipelineRequest(RequestMetaInfo requestMetaInfo, String clientName, Block oldBlock,
        Block newBlock, List<DatanodeID> newNodes) {
      super(requestMetaInfo, clientName);
      this.oldBlock = oldBlock;
      this.newBlock = newBlock;
      this.newNodes = newNodes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Block.writeSafe(out, oldBlock);
      Block.writeSafe(out, newBlock);
      if (newNodes != null) {
        out.writeInt(newNodes.size());
        for (DatanodeID elem : newNodes) {
          DatanodeID.write(out, elem);
        }
      } else {
        out.writeInt(-1);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      oldBlock = Block.readSafe(in);
      newBlock = Block.readSafe(in);
      final int newNodesSize = in.readInt();
      newNodes = (newNodesSize < 0) ? null : new ArrayList<DatanodeID>(newNodesSize);
      for (int i = 0; i < newNodesSize; i++) {
        newNodes.add(DatanodeID.read(in));
      }
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getClientName() {
      return super.getClientName();
    }

    @ThriftField(3)
    public Block getOldBlock() {
      return oldBlock;
    }

    @ThriftField(4)
    public Block getNewBlock() {
      return newBlock;
    }

    @ThriftField(5)
    public List<DatanodeID> getNewNodes() {
      return newNodes;
    }

    @Override
    public Void call(ClientProxyProtocol namenode) throws IOException {
      namenode.updatePipeline(this);
      return null;
    }
  }

  @ThriftStruct
  public static class GetDataTransferProtocolVersionRequest extends Request<Integer> {
    @SuppressWarnings("unused")
    public GetDataTransferProtocolVersionRequest() {
    }

    @ThriftConstructor
    public GetDataTransferProtocolVersionRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo) {
      super(requestMetaInfo);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @Override
    public Integer call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getDataTransferProtocolVersion(this);
    }
  }

  @ThriftStruct
  public static class GetBlockInfoRequest extends Request<BlockInfoResponse> {
    private long blockId;

    @SuppressWarnings("unused")
    public GetBlockInfoRequest() {
    }

    @ThriftConstructor
    public GetBlockInfoRequest(@ThriftField(1) RequestMetaInfo requestMetaInfo,
        @ThriftField(2) long blockId) {
      super(requestMetaInfo);
      this.blockId = blockId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeLong(blockId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      blockId = in.readLong();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public long getBlockId() {
      return blockId;
    }

    @Override
    public BlockInfoResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.getBlockInfo(this);
    }
  }

  @ThriftStruct
  public static class RaidFileRequest extends SourceRequest<Boolean> {
    private String codecId;
    private short expectedSourceReplication;

    @SuppressWarnings("unused")
    public RaidFileRequest() {
    }

    @ThriftConstructor
    public RaidFileRequest(RequestMetaInfo requestMetaInfo, String src, String codecId,
        short expectedSourceReplication) {
      super(requestMetaInfo, src);
      this.codecId = codecId;
      this.expectedSourceReplication = expectedSourceReplication;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeStringOpt(out, codecId);
      out.writeShort(expectedSourceReplication);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      codecId = Text.readStringOpt(in);
      expectedSourceReplication = in.readShort();
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }

    @ThriftField(2)
    public String getSrc() {
      return super.getSrc();
    }

    @ThriftField(3)
    public String getCodecId() {
      return codecId;
    }

    @ThriftField(4)
    public short getExpectedSourceReplication() {
      return expectedSourceReplication;
    }

    @Override
    public Boolean call(ClientProxyProtocol namenode) throws IOException {
      return namenode.raidFile(this);
    }
  }

  @ThriftStruct
  public static class PingRequest extends Request<PingResponse> {
    @SuppressWarnings("unused")
    public PingRequest() {
    }

    @ThriftConstructor
    public PingRequest(@ThriftField(1) RequestMetaInfo metaInfo) {
      super(metaInfo);
    }

    @Override
    public PingResponse call(ClientProxyProtocol namenode) throws IOException {
      return namenode.ping(this);
    }

    @ThriftField(1)
    public RequestMetaInfo getRequestMetaInfo() {
      return super.getRequestMetaInfo();
    }
  }

  ///////////////////////////////////////
  // Helpers
  ///////////////////////////////////////

  static void notNull(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
  }

  static void writeStrings(DataOutput out, List<String> strs) throws IOException {
    if (strs != null) {
      out.writeInt(strs.size());
      for (String str : strs) {
        Text.writeStringOpt(out, str);
      }
    } else {
      out.writeInt(-1);
    }
  }

  static List<String> readStrings(DataInput in) throws IOException {
    final int size = in.readInt();
    List<String> strs = (size < 0) ? null : new ArrayList<String>(size);
    for (int i = 0; i < size; i++) {
      strs.add(Text.readStringOpt(in));
    }
    return strs;
  }
}
