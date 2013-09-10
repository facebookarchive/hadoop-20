package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.io.Text;

/**
 * A block with the name of the file it belongs to.
 * 
 * @author dikang
 */
// DO NOT remove final without consulting {@link WrapperWritable#create(V object)}
@ThriftStruct
public final class LocatedBlockWithFileName extends LocatedBlock {
	private String fullPath = "";
	
	public LocatedBlockWithFileName () {
		
	}

	public LocatedBlockWithFileName (Block block,
																	 DatanodeInfo[] locs,
																	 String fullPath) {
		super(block, locs);
		this.fullPath = fullPath;
	}

  @ThriftConstructor
  public LocatedBlockWithFileName(@ThriftField(1) Block block,
      @ThriftField(2) List<DatanodeInfo> datanodes, @ThriftField(3) long startOffset,
      @ThriftField(4) boolean corrupt, @ThriftField(5) String fileName) {
    super(block, datanodes, startOffset, corrupt);
    this.fullPath = fileName;
  }

  @ThriftField(5)
  public String getFileName() {
		return fullPath;
	}
	
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, fullPath);
		super.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    fullPath = Text.readString(in);
  	super.readFields(in);
  }
}
