package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * A block with the name of the file it belongs to.
 * 
 * @author dikang
 */
public class LocatedBlockWithFileName extends LocatedBlock {
	private String fullPath = "";
	
	public LocatedBlockWithFileName () {
		
	}
	
	public LocatedBlockWithFileName (Block block, 
																	 DatanodeInfo[] locs,
																	 String fullPath) {
		super(block, locs);
		this.fullPath = fullPath;
	}
	
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
