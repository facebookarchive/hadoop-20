package org.apache.hadoop.hdfs;

public interface Constant {
	public static final long MEGA = 0x100000;
	public static final long BUFFERLIMIT = 1000000;
	public static final long NSUBTASKS = 100;
	public static final long BLOCKSIZE = 128;
	public static final long BLOCK_SIZE = 10;

	public static final String ROOT = "/benchmark/";
	public static final String TRASH = ROOT + ".Trash/";
	public static final String INPUT = ROOT + "input/";
	public static final String DFS_INPUT = ROOT + "dfstest-input/";
	public static final String DFS_OUTPUT = ROOT + "dfstest-output/";
	public static final String OUTPUT = ROOT + "output/";
}
