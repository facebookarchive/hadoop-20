
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.util.Arrays;

import com.sun.tools.javac.util.List;

/**
 * File name generator.
 * 
 * Each directory contains not more than a fixed number (filesPerDir) of files
 * and directories. When the number of files in one directory reaches the
 * maximum, the generator creates a new directory and proceeds generating files
 * in it. The generated namespace tree is balanced that is any path to a leaf
 * file is not less than the height of the tree minus one.
 */
public class FileNameGenerator {
	private static final int DEFAULT_FILES_PER_DIRECTORY = 32;

	private int[] pathIndecies = new int[20]; // this will support up to 32**20
												// = 2**100 = 10**30 files
	private String baseDir;
	private String currentDir;
	private int filesPerDirectory;
	private long fileCount;

	FileNameGenerator(String baseDir) {
		this(baseDir, DEFAULT_FILES_PER_DIRECTORY);
	}

	FileNameGenerator(String baseDir, int filesPerDir) {
		new File(baseDir).mkdir();
		this.baseDir = baseDir;
		this.filesPerDirectory = filesPerDir;
		reset();
	}
	

	synchronized String getNextDirName(String prefix) {
		int depth = 0;
		while (pathIndecies[depth] >= 0)
			depth++;
		int level;
		for (level = depth - 1; level >= 0
				&& pathIndecies[level] == filesPerDirectory - 1; level--)
			pathIndecies[level] = 0;
		if (level < 0)
			pathIndecies[depth] = 0;
		else
			pathIndecies[level]++;
		level = 0;
		String next = baseDir;
		while (pathIndecies[level] >= 0)
			next = next + "/" + prefix + pathIndecies[level++];
		return next;
	}

	synchronized String getNextFileName(String fileName) {
		long fNum = fileCount % filesPerDirectory;
		if (fNum == 0) {
			currentDir = getNextDirName("Subdir");
		}
		new File(currentDir).mkdir(); 
		String fn = currentDir + "/" + fileName;
		fileCount++;
		return fn;
	}

	private synchronized void reset() {
		Arrays.fill(pathIndecies, -1);
		fileCount = 0L;
		currentDir = "";
	}

	synchronized int getFilesPerDirectory() {
		return filesPerDirectory;
	}

	synchronized String getCurrentDir() {
		return currentDir;
	}
	
}
