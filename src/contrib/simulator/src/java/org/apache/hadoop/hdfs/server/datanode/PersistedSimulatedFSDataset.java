package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeSet;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.VersionInfo;

public class PersistedSimulatedFSDataset extends FSDataset implements
		FSConstants, FSDatasetInterface {

	/*
	 * FSVolumeSet
	 */
	static class FSVolumeSet extends FSDataset.FSVolumeSet {

		FSVolumeSet(FSVolume[] volumes, int threads) {
			super(volumes, threads);
		}
	}
	


	class FSVolume extends FSDataset.FSVolume{
		
		void getBlocksBeingWrittenInfo(TreeSet<Block> blockSet) {
			if (blocksBeingWritten == null) {
				return;
			}

			File[] blockFiles = blocksBeingWritten.listFiles();
			if (blockFiles == null) {
				return;
			}

			for (int i = 0; i < blockFiles.length; i++) {
				if (!blockFiles[i].isDirectory()) {
					// get each block in the blocksBeingWritten direcotry
					if (Block.isBlockFilename(blockFiles[i])) {
						long genStamp = getGenerationStampFromFile(blockFiles,
								blockFiles[i]);
						Block block = new Block(blockFiles[i],
								getFileSize(blockFiles[i]), genStamp);

						// add this block to block set
						blockSet.add(block);
						if (DataNode.LOG.isDebugEnabled()) {
							DataNode.LOG
									.debug("recoverBlocksBeingWritten for block "
											+ block);
						}
					}
				}
			}
		}

	    FSVolume(File currentDir, Configuration conf, boolean simulated) throws IOException {
	        this.reserved = conf.getLong("dfs.datanode.du.reserved", 0);
	        this.currentDir = currentDir;
	        this.dataDir = new FSDir(currentDir, this, true);
	        boolean supportAppends = datanode.isSupportAppends();
	        File parent = currentDir.getParentFile();

	        this.detachDir = new File(parent, "detach");
	        if (detachDir.exists()) {
	          recoverDetachedBlocks(currentDir, detachDir);
	        }

	        this.tmpDir = new File(parent, "tmp");
	        if (tmpDir.exists()) {
	          FileUtil.fullyDelete(tmpDir);
	        }
	        
	        blocksBeingWritten = new File(parent, "blocksBeingWritten");
	        if (blocksBeingWritten.exists()) {
	          if (supportAppends) {
	            recoverBlocksBeingWritten(blocksBeingWritten);
	          } else {
	            File toDeleteDir = new File(tmpDir.getParent(),
	            DELETE_FILE_EXT + tmpDir.getName());
	            if (tmpDir.renameTo(toDeleteDir)) {
	              asyncDiskService.deleteAsyncFile(this, toDeleteDir);
	            } else {
	              DataNode.LOG.warn("Fail to asynchronosly delete " +
	              tmpDir.getPath() + ". Trying synchronosly deletion.");
	              FileUtil.fullyDelete(tmpDir);
	              DataNode.LOG.warn("Deleted " + tmpDir.getPath());
	            }
	          }
	        }
	        
	        if (!blocksBeingWritten.mkdirs()) {
	          if (!blocksBeingWritten.isDirectory()) {
	            throw new IOException("Mkdirs failed to create " + blocksBeingWritten.toString());
	          }
	        }
	        if (!tmpDir.mkdirs()) {
	          if (!tmpDir.isDirectory()) {
	            throw new IOException("Mkdirs failed to create " + tmpDir.toString());
	          }
	        }
	        if (!detachDir.mkdirs()) {
	          if (!detachDir.isDirectory()) {
	            throw new IOException("Mkdirs failed to create " + detachDir.toString());
	          }
	        }
	        this.usage = new DF(parent, conf);
	        this.dfsUsage = new DU(parent, conf);
	        this.dfsUsage.start();
	      }
		
		// override
		public File addBlock(Block b, File f) throws IOException {
			File blockFile = dataDir.addBlock(b, f);
			File metaFile = getMetaFile(blockFile, b);
			// Ch@nge
			long blocksize = 0;
			File file = getFile(b);
			if (file == null)
				blocksize = b.getNumBytes();
			else {
				blocksize = getFileSize(file);
				b.setNumBytes(blocksize);
			}
			dfsUsage.incDfsUsed(blocksize + metaFile.length());
			return blockFile;
		}
	}


	public static long getFileSize(File file) {
		Scanner scan = null;
		try {
			scan = new Scanner(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long blocksize = scan.nextLong();
		scan.close();
		return blocksize;
	}

	/*
	 * SimulatedFSDir
	 */
	class FSDir extends FSDataset.FSDir{
		
		public FSDir(File dir) throws IOException {
			// ntdo - new constructore for SimulatedFSDir
			this(dir, null, true);
		}

		void getBlockAndFileInfo(LightWeightHashSet<BlockAndFile> blockSet) {
			if (children != null) {
				for (int i = 0; i < children.length; i++) {
					children[i].getBlockAndFileInfo(blockSet);
				}
			}

			File blockFiles[] = dir.listFiles();
			for (int i = 0; i < blockFiles.length; i++) {
				if (Block.isBlockFilename(blockFiles[i])) {
					long genStamp = getGenerationStampFromFile(blockFiles,
							blockFiles[i]);
					long blockSize = getFileSize(blockFiles[i]);
					Block block = new Block(blockFiles[i], blockSize, genStamp);
					blockSet.add(new BlockAndFile(blockFiles[i]
							.getAbsoluteFile(), block));
				}
			}

		}

		public FSDir(File dir, FSVolume volume, boolean simulated) throws IOException {
			this.dir = dir;
			this.children = null;
			if (!dir.exists()) {
				if (!dir.mkdirs()) {
					throw new IOException("Mkdirs failed to create "
							+ dir.toString());
				}
			} else {
				File[] files = dir.listFiles();
				int numChildren = 0;
				for (File file : files) {
					if (file.isDirectory()) {
						numChildren++;
					} else if (Block.isBlockFilename(file)) {
						numBlocks++;
						if (volume != null) {
							long genStamp = getGenerationStampFromFile(files,
									file);
							long blocksize = getFileSize(file);
							synchronized (volumeMap) {
								Block b = new Block(file, blocksize, genStamp);
								volumeMap.put(b, new DatanodeBlockInfo(volume,
										file));
							}
						}
					} else if (isPendingDeleteFilename(file)) {
						DataNode.LOG.info("Scheduling file " + file.toString()
								+ " for deletion");
						String name = file.toString();
						if (file.exists() && !file.delete()) {
							DataNode.LOG
									.warn("Unexpected error trying to delete "
											+ (file.isDirectory() ? "directory "
													: "file ") + name
											+ ". Ignored.");
						} else {
							DataNode.LOG.info("Deleted "
									+ (file.isDirectory() ? "directory "
											: "file ") + name + " at volume "
									+ volume);
						}
					}
				}
				if (numChildren > 0) {
					children = new FSDir[numChildren];
					int curdir = 0;
					for (int idx = 0; idx < files.length; idx++) {
						if (files[idx].isDirectory()) {
							children[curdir] = new FSDir(files[idx], volume, true);
							curdir++;
						}
					}
				}
			}
		}

		public void getBlockInfo(LightWeightHashSet<Block> blockSet) {
			if (children != null) {
				for (int i = 0; i < children.length; i++) {
					children[i].getBlockInfo(blockSet);
				}
			}

			File blockFiles[] = dir.listFiles();
			for (int i = 0; i < blockFiles.length; i++) {
				if (Block.isBlockFilename(blockFiles[i])) {
					long genStamp = getGenerationStampFromFile(blockFiles,
							blockFiles[i]);
					long blocksize = getFileSize(blockFiles[i]);
					blockSet.add(new Block(blockFiles[i], blocksize, genStamp));
				}
			}
		}
	}

	class SimulatedInputStream extends java.io.InputStream {

		byte theRepeatedData = 7;
		long length; // bytes
		int currentPos = 0;
		byte[] data = null;
		File file;
		
		SimulatedInputStream(byte[] data) {
			this.data = data;
			this.length = data.length;
		}
		
		SimulatedInputStream(File file, long l, byte b) {
			this.file = file;
			this.length = l;
			theRepeatedData = b;
		}

		long getLength() {
			return length;
		}

		public int read() throws IOException {
			if (currentPos >= length)
				return -1;
			if (data != null) {
				return data[currentPos++];
			} else {
				currentPos++;
				return theRepeatedData;
			}
		}

		public int read(byte[] b) throws IOException {

			if (b == null) {
				throw new NullPointerException();
			}
			if (b.length == 0) {
				return 0;
			}
			if (currentPos >= length) { // EOF
				return -1;
			}
			int bytesRead = (int) Math.min(b.length, length - currentPos);
			if (data != null) {
				System.arraycopy(data, currentPos, b, 0, bytesRead);
			} else { // all data is zero
				for (int i : b) {
					b[i] = theRepeatedData;
				}
			}
			currentPos += bytesRead;
			return bytesRead;
		}

		public int read(byte[] b, int off, int len) throws IOException {
			if (b == null) {
				throw new NullPointerException();
			}
			if (b.length == 0)
				return 0;
			if (off < 0 || len < 0 || (off + len) > b.length)
				throw new IndexOutOfBoundsException();
			if (len == 0)
				return 0;
			if (currentPos >= length) // EOF
				return -1;

			int bytesRead = (int) Math.min(len, length - currentPos);
			if (data != null) {
				System.arraycopy(data, currentPos, b, off, bytesRead);
			} else
				for (int i = off; i < off + bytesRead; i++)
					b[i] = theRepeatedData;
			currentPos += bytesRead;
			return bytesRead;
		}
	}

	class SimulatedOutputStream extends OutputStream {
		public static final long MAX_BUFFER_SIZE = 64 * 1024;

		long length = 0;
		File file;
		long bufferSize;
		long count = 0;
		OutputStreamWriter out = null; 
		
		SimulatedOutputStream() {
		}
		
		SimulatedOutputStream(File file) {
			this.file = file;
			try {
				out = new OutputStreamWriter(new FileOutputStream(this.file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} 
		}
		
		long getLength() {
			return length;
		}
		
		void setLength(long length) {
			this.length = length;
		}

		@Override
		public void write(int arg0) throws IOException {
			// new change 
			length += 1;
			bufferSize += 1; 
			if (bufferSize > MAX_BUFFER_SIZE) {
				bufferSize = length % MAX_BUFFER_SIZE;
				flush(); 
			}
		}

		@Override
		public void write(byte[] b) throws IOException {
			for (int i = 0; i < b.length; i++) 
				write(b.length); 
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			for (int i = 0; i < len; i++) 
				write(len); 	
		}
		
		// make the flush method
		public void flush() {
			count++; 
			try {
				out = new OutputStreamWriter(new FileOutputStream(this.file)); 
				out.write(String.valueOf(count * MAX_BUFFER_SIZE));
				out.close(); 
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
		}

		public void close() {
			try {
				out = new OutputStreamWriter(new FileOutputStream(this.file)); 
				out.write(String.valueOf(this.length));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					out.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	/*
	 * data for checksum
	 */
	static byte[] nullCrcFileData;
	{

		DataChecksum checksum = DataChecksum.newDataChecksum(
				DataChecksum.CHECKSUM_NULL, 16 * 1024);
		byte[] nullCrcHeader = checksum.getHeader();
		nullCrcFileData = new byte[2 + nullCrcHeader.length];
		nullCrcFileData[0] = (byte) ((FSDataset.METADATA_VERSION >>> 8) & 0xff);
		nullCrcFileData[1] = (byte) (FSDataset.METADATA_VERSION & 0xff);
		for (int i = 0; i < nullCrcHeader.length; i++) {
			nullCrcFileData[i + 2] = nullCrcHeader[i];
		}
	}

	
	public PersistedSimulatedFSDataset(DataNode datanode, DataStorage storage,
			Configuration conf) throws IOException {
		super(datanode, storage, conf);
	}

	public PersistedSimulatedFSDataset() {
		super();
	}

	public void initialize(DataNode datanode, DataStorage storage, Configuration conf) throws IOException {
		this.datanode = datanode;
		this.maxBlocksPerDir = conf.getInt("dfs.datanode.numblocks", 64);
		volumeMap = new HashMap<Block, DatanodeBlockInfo>();
		ongoingCreates = new HashMap<Block, ActiveFile>();
		File[] roots = new File[storage.getNumStorageDirs()];
		for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
			roots[idx] = storage.getStorageDir(idx).getCurrentDir();
		}
		asyncDiskService = new FSDatasetAsyncDiskService(roots, conf);
		// create volumes with threads
		// each thread would go with on volume
		FSVolume[] volArray = createVolumes(storage, conf);
		int threads = conf.getInt("dfs.datanode.blockscanner.threads", 1);
		volumes = new FSVolumeSet(volArray, threads);
		registerMBean(storage.getStorageID());
	}

	/*
	 * VolumeThread : running thread for each volume
	 */
	private class VolumeThread extends Thread {
		private File volumeDir;
		private Configuration conf;
		private FSVolume volume;
		private boolean hasError = false;

		private VolumeThread(File volumeDir, Configuration conf) {
			this.volumeDir = volumeDir;
			this.conf = conf;
		}

		public void run() {
			DataNode.LOG.info("Start building volume for " + volumeDir);
			try {
				volume = new FSVolume(volumeDir, conf, true);
			} catch (IOException ioe) {
				DataNode.LOG
						.error("Error building volume at " + volumeDir, ioe);
				hasError = true;
			}
			DataNode.LOG.info("Finish building volume for " + volumeDir);
		}
	}

	private FSVolume[] createVolumes(DataStorage storage, Configuration conf)
			throws IOException {
		VolumeThread[] scanners = new VolumeThread[storage.getNumStorageDirs()];
		for (int idx = 0; idx < scanners.length; idx++) {
			scanners[idx] = new VolumeThread(storage.getStorageDir(idx)
					.getCurrentDir(), conf);
			scanners[idx].start();
		}
		boolean hasError = false;
		FSVolume[] volumes = new FSVolume[scanners.length];
		for (int idx = 0; idx < scanners.length; idx++) {
			try {
				scanners[idx].join();
			} catch (InterruptedException e) {
				throw (InterruptedIOException) new InterruptedIOException()
						.initCause(e);
			}
			if (!hasError && scanners[idx].hasError) {
				hasError = true;
			}
			if (!hasError) {
				volumes[idx] = scanners[idx].volume;
			}
		}
		if (hasError) {
			throw new IOException("Error creating volumes");
		}
		return volumes;
	}

	void registerMBean(final String storageId) {
		// We wrap to bypass standard mbean naming convetion.
		// This wraping can be removed in java 6 as it is more flexible in
		// package naming for mbeans and their impl.
		StandardMBean bean;
		String storageName;
		if (storageId == null || storageId.equals("")) {// Temp fix for the
														// uninitialized storage
			storageName = "UndefinedStorageId" + rand.nextInt();
		} else {
			storageName = storageId;
		}
		try {
			bean = new StandardMBean(this, FSDatasetMBean.class);
			mbeanName = MBeanUtil.registerMBean("DataNode", "FSDatasetState-"
					+ storageName, bean);
			versionBeanName = VersionInfo.registerJMX("DataNode");
		} catch (NotCompliantMBeanException e) {
			e.printStackTrace();
		}

		DataNode.LOG.info("Registered FSDatasetStatusMBean");
	}




	@Override
	public long getMetaDataLength(Block b) throws IOException {
		// TODO Auto-generated method stub
		return nullCrcFileData.length;
	}

	@Override
	public MetaDataInputStream getMetaDataInputStream(Block b)
			throws IOException {
		// TODO Auto-generated method stub
		SimulatedInputStream metaFile = new SimulatedInputStream(
				nullCrcFileData);
		return new MetaDataInputStream(metaFile, metaFile.getLength());
	}


	@Override
	public long getLength(Block b) throws IOException {
		// TODO Auto-generated method stub
		// long blocksize = DEFAULT_BLOCKSIZE;
		long blocksize = getFileSize(getBlockFile(b));
		b.setNumBytes(blocksize);
		return blocksize;
	}

	@Override
	public Block getStoredBlock(long blkid) throws IOException {
		// TODO Auto-generated method stub
		lock.readLock().lock();
		try {
			File blockfile = findBlockFile(blkid);
			if (blockfile == null) {
				return null;
			}
			File metafile = findMetaFile(blockfile);
			long blocksize = getFileSize(blockfile);
			return new Block(blkid, blocksize, parseGenerationStamp(blockfile,
					metafile));
		} finally {
			lock.readLock().unlock();
		}
	}


	@Override
	public InputStream getBlockInputStream(Block b) throws IOException {
		// TODO Auto-generated method stub
		File blockfile = getBlockFile(b);
		return new SimulatedInputStream(blockfile, b.getNumBytes(), (byte) 'a');
	}

	@Override
	public InputStream getBlockInputStream(Block b, long seekOffset)
			throws IOException {
		File blockFile = getBlockFile(b);
		InputStream out = new SimulatedInputStream(blockFile, b.getNumBytes(),
				(byte) 'a');
		out.skip(seekOffset);
		return out;
	}

	@Override
	public BlockInputStreams getTmpInputStreams(Block b, long blkoff, long ckoff)
			throws IOException {
		// TODO Auto-generated method stub
		lock.readLock().lock();
		try {
			DatanodeBlockInfo info = volumeMap.get(b);
			if (info == null) {
				throw new IOException("Block " + b
						+ " does not exist in volumeMap.");
			}
			FSVolume v = (FSVolume) info.getVolume();
			File blockFile = v.getTmpFile(b);
			InputStream blockInFile = new SimulatedInputStream(blockFile,
					b.getNumBytes(), (byte) 'a');
			blockInFile.skip(blkoff);
			File metaFile = getMetaFile(blockFile, b);
			RandomAccessFile metaInFile = new RandomAccessFile(metaFile, "r");
			if (ckoff > 0) {
				metaInFile.seek(ckoff);
			}
			// return new BlockInputStreams(new FileInputStream(
			// blockInFile.getFD()), new FileInputStream(
			// metaInFile.getFD()));

			return new BlockInputStreams(blockInFile, new FileInputStream(
					metaInFile.getFD()));
		} finally {
			lock.readLock().unlock();
		}
	}

	public File getFile(Block b) {
		lock.readLock().lock();
		try {
			DatanodeBlockInfo info = volumeMap.get(b);
			if (info != null) {
				return info.getFile();
			}
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}
	

	@Override
	// the same

	public BlockWriteStreams writeToBlock(Block b, boolean isRecovery,
			boolean replicationRequest) throws IOException {
		
		// TODO Auto-generated method stub
		if (isValidBlock(b)) {
			if (!isRecovery) {
				throw new BlockAlreadyExistsException("Block " + b
						+ " is valid, and cannot be written to.");
			}
			detachBlock(b, 1);
		}
		long blockSize = b.getNumBytes();

		//
		// Serialize access to /tmp, and check if file already there.
		//
		File f = null;
		List<Thread> threads = null;
		lock.writeLock().lock();
		try {
			//
			// Is it already in the create process?
			//
			ActiveFile activeFile = ongoingCreates.get(b);
			if (activeFile != null) {
				f = activeFile.file;
				threads = activeFile.threads;

				if (!isRecovery) {
					throw new BlockAlreadyExistsException(
							"Block "
									+ b
									+ " has already been started (though not completed), and thus cannot be created.");
				} else {
					for (Thread thread : threads) {
						thread.interrupt();
					}
				}
				ongoingCreates.remove(b);
			}
			FSVolume v = null;
			if (!isRecovery) {
				v = (FSVolume) volumes.getNextVolume(blockSize);
				// create temporary file to hold block in the designated volume
				f = createTmpFile(v, b, replicationRequest);
				volumeMap.put(b, new DatanodeBlockInfo(v));
			} else if (f != null) {
				DataNode.LOG.info("Reopen already-open Block for append " + b);
				// create or reuse temporary file to hold block in the
				// designated volume
				v = (FSVolume) volumeMap.get(b).getVolume();
				volumeMap.put(b, new DatanodeBlockInfo(v));
			} else {
				// reopening block for appending to it.
				DataNode.LOG.info("Reopen Block for append " + b);
				v = (FSVolume) volumeMap.get(b).getVolume();
				f = createTmpFile(v, b, replicationRequest);
				File blkfile = getBlockFile(b);
				File oldmeta = getMetaFile(b);
				File newmeta = getMetaFile(f, b);

				// rename meta file to tmp directory
				DataNode.LOG.debug("Renaming " + oldmeta + " to " + newmeta);
				if (!oldmeta.renameTo(newmeta)) {
					throw new IOException("Block " + b + " reopen failed. "
							+ " Unable to move meta file  " + oldmeta
							+ " to tmp dir " + newmeta);
				}

				// rename block file to tmp directory
				DataNode.LOG.debug("Renaming " + blkfile + " to " + f);
				if (!blkfile.renameTo(f)) {
					if (!f.delete()) {
						throw new IOException("Block " + b + " reopen failed. "
								+ " Unable to remove file " + f);
					}
					if (!blkfile.renameTo(f)) {
						throw new IOException("Block " + b + " reopen failed. "
								+ " Unable to move block file " + blkfile
								+ " to tmp dir " + f);
					}
				}
				volumeMap.put(b, new DatanodeBlockInfo(v));
			}
			if (f == null) {
				DataNode.LOG.warn("Block " + b + " reopen failed "
						+ " Unable to locate tmp file.");
				throw new IOException("Block " + b + " reopen failed "
						+ " Unable to locate tmp file.");
			}
			ongoingCreates.put(b, new ActiveFile(f, threads));
		} finally {
			lock.writeLock().unlock();
		}

		try {
			if (threads != null) {
				for (Thread thread : threads) {
					thread.join();
				}
			}
		} catch (InterruptedException e) {
			throw new IOException("Recovery waiting for thread interrupted.");
		}

		//
		// Finally, allow a writer to the block file
		// REMIND - mjc - make this a filter stream that enforces a max
		// block size, so clients can't go crazy
		//
		File metafile = getMetaFile(f, b);
		DataNode.LOG.debug("writeTo blockfile is " + f + " of size "
				+ f.length());
		DataNode.LOG.debug("writeTo metafile is " + metafile + " of size "
				+ metafile.length());
		return createBlockWriteStreams(f, metafile);
	}

	private BlockWriteStreams createBlockWriteStreams(File f, File metafile)
			throws IOException {
		return new BlockWriteStreams(new SimulatedOutputStream(f),
				new SimulatedOutputStream(metafile));

	}

	  public void updateBlock(Block oldblock, Block newblock) throws IOException {
		    if (oldblock.getBlockId() != newblock.getBlockId()) {
		      throw new IOException("Cannot update oldblock (=" + oldblock
		          + ") to newblock (=" + newblock + ").");
		    }

		    // Protect against a straggler updateblock call moving a block backwards
		    // in time.
		    boolean isValidUpdate =
		      (newblock.getGenerationStamp() > oldblock.getGenerationStamp()) ||
		      (newblock.getGenerationStamp() == oldblock.getGenerationStamp() &&
		       newblock.getNumBytes() == oldblock.getNumBytes());

		    if (!isValidUpdate) {
		      throw new IOException(
		        "Cannot update oldblock=" + oldblock +
		        " to newblock=" + newblock + " since generation stamps must " +
		        "increase, or else length must not change.");
		    }
		    
		    for(;;) {
		      final List<Thread> threads = tryUpdateBlock(oldblock, newblock);
		      if (threads == null) {
		        return;
		      }

		      interruptAndJoinThreads(threads);
		    }
		  }
	  
	  private boolean interruptAndJoinThreads(List<Thread> threads) {
		    // interrupt and wait for all ongoing create threads
		    for(Thread t : threads) {
		      t.interrupt();
		    }
		    for(Thread t : threads) {
		      try {
		        t.join();
		      } catch (InterruptedException e) {
		        DataNode.LOG.warn("interruptOngoingCreates: t=" + t, e);
		        return false;
		      }
		    }
		    return true;
		  }
	  private ArrayList<Thread> getActiveThreads(Block block) {
		    lock.writeLock().lock();
		    try {
		      //check ongoing create threads
		      final ActiveFile activefile = ongoingCreates.get(block);
		      if (activefile != null && !activefile.threads.isEmpty()) {
		        //remove dead threads
		        for(Iterator<Thread> i = activefile.threads.iterator(); i.hasNext(); ) {
		          final Thread t = i.next();
		          if (!t.isAlive()) {
		            i.remove();
		          }
		        }
		  
		        //return living threads
		        if (!activefile.threads.isEmpty()) {
		          return new ArrayList<Thread>(activefile.threads);
		        }
		      }
		    } finally {
		      lock.writeLock().unlock();
		    }
		    return null;
		  }  

	private List<Thread> tryUpdateBlock(Block oldblock, Block newblock)
			throws IOException {
		lock.writeLock().lock();
		try {
			// check ongoing create threads
			final ArrayList<Thread> activeThreads = getActiveThreads(oldblock);
			if (activeThreads != null) {
				return activeThreads;
			}

			// No ongoing create threads is alive. Update block.
			File blockFile = findBlockFile(oldblock.getBlockId());
			if (blockFile == null) {
				throw new IOException("Block " + oldblock + " does not exist.");
			}

			File oldMetaFile = findMetaFile(blockFile);
			long oldgs = parseGenerationStamp(blockFile, oldMetaFile);

			// rename meta file to a tmp file
			File tmpMetaFile = new File(oldMetaFile.getParent(),
					oldMetaFile.getName() + "_tmp"
							+ newblock.getGenerationStamp());
			if (!oldMetaFile.renameTo(tmpMetaFile)) {
				throw new IOException("Cannot rename block meta file to "
						+ tmpMetaFile);
			}

			if (oldgs > newblock.getGenerationStamp()) {
				throw new IOException("Cannot update block (id="
						+ newblock.getBlockId() + ") generation stamp from "
						+ oldgs + " to " + newblock.getGenerationStamp());
			}

			// update length
			if (newblock.getNumBytes() > oldblock.getNumBytes()) {
				throw new IOException("Cannot update block file (=" + blockFile
						+ ") length from " + oldblock.getNumBytes() + " to "
						+ newblock.getNumBytes());
			}
		    //rename meta file to a tmp file
			
			if (newblock.getNumBytes() < oldblock.getNumBytes()) {
				newTruncateBlock(blockFile, tmpMetaFile,
						oldblock.getNumBytes(), newblock.getNumBytes());
				oldblock.setNumBytes(newblock.getNumBytes());
			      ActiveFile file = ongoingCreates.get(oldblock);
			      if (file != null) {
			        file.setVisibleLength(newblock.getNumBytes());
			      }
			}
			
			File newMetaFile = getMetaFile(blockFile, newblock);
			if (!tmpMetaFile.renameTo(newMetaFile)) {
				throw new IOException("Cannot rename tmp meta file to "
						+ newMetaFile);
			}

			updateBlockMap(ongoingCreates, oldblock, newblock);
			updateBlockMap(volumeMap, oldblock, newblock);

			// paranoia! verify that the contents of the stored block that
			// matches the block file on disk.
			// ch@nge : no need to check the metadata file with the file on disk
			validateBlockMetadata(newblock);
			return null;
		} finally {
			lock.writeLock().unlock();
		}
	}

	static void newTruncateBlock(File blockFile, File metaFile,
			long oldlen, long newlen) throws IOException {
		if (newlen == oldlen) {
			return;
		}
		if (newlen > oldlen) {
			throw new IOException("Cannout truncate block to from oldlen (="
					+ oldlen + ") to newlen (=" + newlen + ")");
		}

		DataOutputStream out = new DataOutputStream(new FileOutputStream(
				blockFile));
		out.writeLong(newlen);
		out.close();
	}



	public Block[] getBlockReport() {
		// TODO Auto-generated method stub
		LightWeightHashSet<Block> blockSet = new LightWeightHashSet<Block>();
		volumes.getBlockInfo(blockSet);
		Block blockTable[] = new Block[blockSet.size()];
		int i = 0;
		for (Iterator<Block> it = blockSet.iterator(); it.hasNext(); i++) {
			Block theBlock = (Block) it.next();
			try {
				long blocksize = getFileSize(getBlockFile(theBlock));
				theBlock.setNumBytes(blocksize);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			blockTable[i] = theBlock;
		}
		return blockTable;
	}

	@Override
	public long getChannelPosition(Block b, BlockWriteStreams streams)
			throws IOException {
		// TODO Auto-generated method stub
		return ((SimulatedOutputStream) streams.dataOut).getLength();
	}

	@Override
	public void setChannelPosition(Block b, BlockWriteStreams streams,
			long dataOffset, long ckOffset) throws IOException {
		// TODO Auto-generated method stub
		long size = 0;
		lock.readLock().lock();
		try {
			// FSVolume vol = volumeMap.get(b).getVolume();
			// size = vol.getTmpFile(b).length();
			File blockFile = getBlockFile(b);
			size = getFileSize(blockFile);
			// size = DEFAULT_BLOCKSIZE;
		} finally {
			lock.readLock().unlock();
		}
		if (size < dataOffset) {
			String msg = "Trying to change block file offset of block " + b
					+ " to " + dataOffset + " but actual size of file is "
					+ size;
			throw new IOException(msg);
		}
		// FileOutputStream file = (FileOutputStream) streams.dataOut;
		// file.getChannel().position(dataOffset);
		((SimulatedOutputStream) streams.dataOut).setLength(dataOffset);
		b.setNumBytes(dataOffset);

		FileOutputStream file = (FileOutputStream) streams.checksumOut;
		file.getChannel().position(ckOffset);
	}
	public void invalidate(Block[] invalidBlks) throws IOException {
		// TODO Auto-generated method stub
		boolean error = false;
		for (int i = 0; i < invalidBlks.length; i++) {
			File f = null;
			FSVolume v;
			lock.writeLock().lock();
			try {
				f = getFile(invalidBlks[i]);
				DatanodeBlockInfo dinfo = volumeMap.get(invalidBlks[i]);
				if (dinfo == null) {
					DataNode.LOG
							.warn("Unexpected error trying to delete block "
									+ invalidBlks[i]
									+ ". BlockInfo not found in volumeMap.");
					error = true;
					continue;
				}
				v = (FSVolume) dinfo.getVolume();
				if (f == null) {
					DataNode.LOG
							.warn("Unexpected error trying to delete block "
									+ invalidBlks[i]
									+ ". Block not found in blockMap."
									+ ((v == null) ? " "
											: " Block found in volumeMap."));
					error = true;
					continue;
				}
				if (v == null) {
					DataNode.LOG
							.warn("Unexpected error trying to delete block "
									+ invalidBlks[i]
									+ ". No volume for this block."
									+ " Block found in blockMap. " + f + ".");
					error = true;
					continue;
				}
				File parent = f.getParentFile();
				if (parent == null) {
					DataNode.LOG
							.warn("Unexpected error trying to delete block "
									+ invalidBlks[i]
									+ ". Parent not found for file " + f + ".");
					error = true;
					continue;
				}
				v.clearPath(parent);
				volumeMap.remove(invalidBlks[i]);
			} finally {
				lock.writeLock().unlock();
			}
			File metaFile = getMetaFile(f, invalidBlks[i]);
			long dfsBytes = getFileSize(f) + metaFile.length();

			// rename the files to be deleted
			// for safety we add prefix instead of suffix,
			// so the valid block files still start with "blk_"
			File blockFileRenamed = new File(f.getParent() + File.separator
					+ "toDelete." + f.getName());
			File metaFileRenamed = new File(metaFile.getParent()
					+ File.separator + "toDelete." + metaFile.getName());

			if ((!f.renameTo(blockFileRenamed))
					|| (!metaFile.renameTo(metaFileRenamed))) {
				DataNode.LOG.warn("Unexpected error trying to delete block "
						+ invalidBlks[i]
						+ ". Cannot rename files for deletion.");
				error = true;
				continue;
			}
			datanode.notifyNamenodeDeletedBlock(invalidBlks[i]);
			// Delete the block asynchronously to make sure we can do it fast
			// enough
			DataNode.LOG.info("Scheduling block "
					+ invalidBlks[i].getBlockName() + " file " + f
					+ " for deletion");
			blockFileRenamed.delete();
			metaFileRenamed.delete();
			f.delete();
			metaFile.delete();
			v.decDfsUsed(dfsBytes);
			DataNode.LOG.info("Deleted block " + invalidBlks[i].getBlockName()
					+ " at file " + f);

		}
		if (error) {
			throw new IOException("Error in deleting blocks.");
		}
	}
	
	public List<File> getCurrentDirs(FSDataset obj) {
		  PersistedSimulatedFSDataset dataset = (PersistedSimulatedFSDataset) obj;
			List<File> result = new ArrayList<File>();
			for (FSDataset.FSVolume vol : dataset.volumes.volumes) { 
				result.add(vol.getDir()); 
			}
			return result; 
	}
}
