/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.LexicographicalComparerHolder;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ResourceCalculatorPlugin.ProcResourceValues;

public class BlockMapOutputBuffer<K extends BytesWritable, V extends BytesWritable>
    implements BlockMapOutputCollector<K, V> {

  private static final Log LOG = LogFactory.getLog(BlockMapOutputBuffer.class.getName());

  private final Partitioner<K, V> partitioner;
  private final int partitions;
  private final JobConf job;
  private final TaskReporter reporter;
  private final Class<K> keyClass;
  private final Class<V> valClass;
  private final int softBufferLimit;
  // Compression for map-outputs
  private CompressionCodec codec = null;
  // main output buffer
  private byte[] kvbuffer;
  private int kvBufferSize;
  // spill accounting
  private volatile int numSpills = 0;
  // number of spills for big records
  private volatile int numBigRecordsSpills = 0;
  private volatile int numBigRecordsWarnThreshold = 500;

  private final FileSystem localFs;
  private final FileSystem rfs;
  private final Counters.Counter mapOutputByteCounter;
  private final Counters.Counter mapOutputRecordCounter;
  private MapSpillSortCounters mapSpillSortCounter;

  private MapTask task;
  private ReducePartition<K, V>[] reducePartitions;
  private ArrayList<SpillRecord> indexCacheList;
  // an array of memory segments, one for each reduce partition.
  private Segment<K,V>[] inMemorySegments;
  private boolean hasInMemorySpill;
  private boolean lastSpillInMem;

  private int totalIndexCacheMemory;
  private static final int INDEX_CACHE_MEMORY_LIMIT = 2 * 1024 * 1024;
  private final MemoryBlockAllocator memoryBlockAllocator;

  @SuppressWarnings( { "unchecked", "deprecation" })
  public BlockMapOutputBuffer(TaskUmbilicalProtocol umbilical, JobConf job,
      TaskReporter reporter, MapTask task) throws IOException,
      ClassNotFoundException {
    this.task = task;
    this.job = job;
    this.reporter = reporter;
    localFs = FileSystem.getLocal(job);
    partitions = job.getNumReduceTasks();
    indexCacheList = new ArrayList<SpillRecord>();
    if (partitions > 0) {
      partitioner = (Partitioner<K, V>) ReflectionUtils.newInstance(job
          .getPartitionerClass(), job);
    } else {
      partitioner = new Partitioner() {
        @Override
        public int getPartition(Object key, Object value, int numPartitions) {
          return -1;
        }

        @Override
        public void configure(JobConf job) {
        }
      };
    }
    rfs = ((LocalFileSystem) localFs).getRaw();

    float spillper = job.getFloat("io.sort.spill.percent", (float) 0.9);
    if (spillper > (float) 1.0 || spillper < (float) 0.0) {
      LOG.error("Invalid \"io.sort.spill.percent\": " + spillper);
      spillper = 0.8f;
    }
    
    lastSpillInMem = job.getBoolean("mapred.map.lastspill.memory", true);
    numBigRecordsWarnThreshold =
        job.getInt("mapred.map.bigrecord.spill.warn.threshold", 500);

    int sortmb = job.getInt("io.sort.mb", 100);
    boolean localMode = job.get("mapred.job.tracker", "local").equals("local");
    if (localMode) {
      sortmb = job.getInt("io.sort.mb.localmode", 100);
    }
    if ((sortmb & 0x7FF) != sortmb) {
      throw new IOException("Invalid \"io.sort.mb\": " + sortmb);
    }
    LOG.info("io.sort.mb = " + sortmb);
    // buffers and accounting
    kvBufferSize = sortmb << 20;
    kvbuffer = new byte[kvBufferSize];
    softBufferLimit = (int) (kvbuffer.length * spillper);
    // k/v serialization
    keyClass = (Class<K>) job.getMapOutputKeyClass();
    valClass = (Class<V>) job.getMapOutputValueClass();
    if (!BytesWritable.class.isAssignableFrom(keyClass)
        || !BytesWritable.class.isAssignableFrom(valClass)) {
      throw new IOException(this.getClass().getName()
          + "  only support " + BytesWritable.class.getName()
          + " as key and value classes, MapOutputKeyClass is "
          + keyClass.getName() + ", MapOutputValueClass is "
          + valClass.getName());
    }

    int numMappers = job.getNumMapTasks();
    memoryBlockAllocator =
        new MemoryBlockAllocator(kvBufferSize, softBufferLimit, numMappers,
            partitions, this);

    // counters
    mapOutputByteCounter = reporter.getCounter(MAP_OUTPUT_BYTES);
    mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);
    mapSpillSortCounter = new MapSpillSortCounters(reporter);

    reducePartitions = new ReducePartition[partitions];
    inMemorySegments = new Segment[partitions];
    for (int i = 0; i < partitions; i++) {
      reducePartitions[i] = new ReducePartition(i, this.memoryBlockAllocator,
          this.kvbuffer, this, this.reporter);
    }     
    // compression
    if (job.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass = job
          .getMapOutputCompressorClass(DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    }
  }

  private TaskAttemptID getTaskID() {
    return task.getTaskID();
  }

  public void collect(K key, V value, int partition) throws IOException {
    reporter.progress();
    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
          + keyClass.getName() + ", recieved " + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
          + valClass.getName() + ", recieved " + value.getClass().getName());
    }
    int collected = reducePartitions[partition].collect(key, value);
    mapOutputRecordCounter.increment(1);
    mapOutputByteCounter.increment(collected);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void collect(K key, V value) throws IOException {
    collect(key, value, partitioner.getPartition(key, value,
        partitions));
  }

  /*
   * return the value of ProcResourceValues for later use
   */
  protected ProcResourceValues sortReduceParts() {
    long sortStartMilli = System.currentTimeMillis();
    ProcResourceValues sortStartProcVals =
        task.getCurrentProcResourceValues();
    long sortStart = task.jmxThreadInfoTracker.getTaskCPUTime("MAIN_TASK");
    // sort
    for (int i = 0; i < reducePartitions.length; i++) {
      reducePartitions[i].groupOrSort();
    }
    long sortEndMilli = System.currentTimeMillis();
    ProcResourceValues sortEndProcVals =
        task.getCurrentProcResourceValues();
    long sortEnd = task.jmxThreadInfoTracker.getTaskCPUTime("MAIN_TASK");
    mapSpillSortCounter.incCountersPerSort(sortStartProcVals,
        sortEndProcVals, sortEndMilli - sortStartMilli);
    mapSpillSortCounter.incJVMCPUPerSort(sortStart, sortEnd);
    return sortEndProcVals;
  }

  @Override
  public void sortAndSpill() throws IOException {
    ProcResourceValues sortEndProcVals = sortReduceParts();
    long sortEndMilli = System.currentTimeMillis();
    long spillStart = task.jmxThreadInfoTracker.getTaskCPUTime("MAIN_TASK");
    // spill
    FSDataOutputStream out = null;
    long spillBytes = 0;
    try {
      // create spill file
      final SpillRecord spillRec = new SpillRecord(partitions);
      final Path filename =
          task.mapOutputFile
              .getSpillFileForWrite(getTaskID(), numSpills,
                  this.memoryBlockAllocator.getEstimatedSize());
      out = rfs.create(filename);
      for (int i = 0; i < partitions; ++i) {
        IndexRecord rec =
            reducePartitions[i].spill(job, out, keyClass, valClass,
                codec, task.spilledRecordsCounter);
        // record offsets
        spillBytes += rec.partLength;
        spillRec.putIndex(rec, i);
      }

      if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
        // create spill index file
        Path indexFilename =
            task.mapOutputFile.getSpillIndexFileForWrite(getTaskID(),
                numSpills, partitions
                    * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, job);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
            spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      LOG.info("Finished spill " + numSpills);
      ++numSpills;
    } finally {
      if (out != null)
        out.close();
    }

    long spillEndMilli = System.currentTimeMillis();
    ProcResourceValues spillEndProcVals =
        task.getCurrentProcResourceValues();
    long spillEnd = task.jmxThreadInfoTracker.getTaskCPUTime("MAIN_TASK");
    mapSpillSortCounter.incCountersPerSpill(sortEndProcVals,
        spillEndProcVals, spillEndMilli - sortEndMilli, spillBytes);
    mapSpillSortCounter.incJVMCPUPerSpill(spillStart, spillEnd);
  }

  public void spillSingleRecord(K key, V value, int part)
      throws IOException {

    ProcResourceValues spillStartProcVals =
        task.getCurrentProcResourceValues();
    long spillStartMilli = System.currentTimeMillis();
    long spillStart = task.jmxThreadInfoTracker.getTaskCPUTime("MAIN_TASK");
    // spill
    FSDataOutputStream out = null;
    long spillBytes = 0;
    try {
      // create spill file
      final SpillRecord spillRec = new SpillRecord(partitions);
      final Path filename =
          task.mapOutputFile.getSpillFileForWrite(getTaskID(),
              numSpills, key.getLength() + value.getLength());
      out = rfs.create(filename);
      IndexRecord rec = new IndexRecord();
      for (int i = 0; i < partitions; ++i) {
        IFile.Writer<K, V> writer = null;
        try {
          long segmentStart = out.getPos();
          // Create a new codec, don't care!
          writer =
              new IFile.Writer<K, V>(job, out, keyClass, valClass,
                  codec, task.spilledRecordsCounter);
          if (i == part) {
            final long recordStart = out.getPos();
            writer.append(key, value);
            // Note that our map byte count will not be accurate with
            // compression
            mapOutputByteCounter
                .increment(out.getPos() - recordStart);
          }
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          spillBytes += writer.getCompressedLength();
          spillRec.putIndex(rec, i);
          writer = null;
        } catch (IOException e) {
          if (null != writer)
            writer.close();
          throw e;
        }
      }

      if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
        // create spill index file
        Path indexFilename =
            task.mapOutputFile.getSpillIndexFileForWrite(getTaskID(),
                numSpills, partitions
                    * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, job);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
            spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      
      LOG.info("Finished spill big record " + numBigRecordsSpills);
      ++numBigRecordsSpills;
      ++numSpills;
    } finally {
      if (out != null)
        out.close();
    }

    long spillEndMilli = System.currentTimeMillis();
    ProcResourceValues spillEndProcVals =
        task.getCurrentProcResourceValues();
    mapSpillSortCounter.incCountersPerSpill(spillStartProcVals,
        spillEndProcVals, spillEndMilli - spillStartMilli, spillBytes);
    long spillEnd = task.jmxThreadInfoTracker.getTaskCPUTime("MAIN_TASK");
    mapSpillSortCounter.incJVMCPUPerSpill(spillStart, spillEnd);
    mapSpillSortCounter.incSpillSingleRecord();
  }
  
  public synchronized void flush() throws IOException, ClassNotFoundException,
      InterruptedException {
    if (numSpills > 0 && lastSpillInMem) {
      // if there is already one spills, we can try to hold this last spill in
      // memory.
      sortReduceParts();
      for (int i = 0; i < partitions; i++) {
        this.inMemorySegments[i] =
            new Segment<K, V>(this.reducePartitions[i].getIReader(),
                true);
      }
      hasInMemorySpill=true;
    } else {
      sortAndSpill();      
    }
    long mergeStartMilli = System.currentTimeMillis();
    ProcResourceValues mergeStartProcVals = task.getCurrentProcResourceValues();
    long mergeStart = task.jmxThreadInfoTracker.getTaskCPUTime("MAIN_TASK");
    mergeParts();
    long mergeEndMilli = System.currentTimeMillis();
    ProcResourceValues mergeEndProcVals = task.getCurrentProcResourceValues();
    long mergeEnd = task.jmxThreadInfoTracker.getTaskCPUTime("MAIN_TASK");
    mapSpillSortCounter.incMergeCounters(mergeStartProcVals, mergeEndProcVals,
        mergeEndMilli - mergeStartMilli);
    mapSpillSortCounter.incJVMCPUMerge(mergeStart, mergeEnd);
  }

  private void mergeParts() throws IOException, InterruptedException,
      ClassNotFoundException {
    // get the approximate size of the final output/index files
    long finalOutFileSize = 0;
    long finalIndexFileSize = 0;
    final Path[] filename = new Path[numSpills];
    final TaskAttemptID mapId = getTaskID();

    for (int i = 0; i < numSpills; i++) {
      filename[i] = task.mapOutputFile.getSpillFile(mapId, i);
      finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
    }

    for (Segment<K, V> segement : this.inMemorySegments) {
      if(segement != null) {
        finalOutFileSize += segement.getLength();        
      }
    }

    // the spill is the final output
    if (numSpills == 1 && !hasInMemorySpill) {
      Path outFile = new Path(filename[0].getParent(), "file.out");
      rfs.rename(filename[0], outFile);
      if (indexCacheList.size() == 0) {
        rfs.rename(task.mapOutputFile.getSpillIndexFile(mapId, 0), new Path(
            filename[0].getParent(), "file.out.index"));
      } else {
        indexCacheList.get(0).writeToFile(
            new Path(filename[0].getParent(), "file.out.index"), job);
      }
      return;
    }

    // read in paged indices
    for (int i = indexCacheList.size(); i < numSpills; ++i) {
      Path indexFileName = task.mapOutputFile.getSpillIndexFile(mapId, i);
      indexCacheList.add(new SpillRecord(indexFileName, job));
    }

    // make correction in the length to include the file header
    // lengths for each partition
    finalOutFileSize += partitions * MapTask.APPROX_HEADER_LENGTH;
    finalIndexFileSize = partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
    Path finalOutputFile = task.mapOutputFile.getOutputFileForWrite(mapId,
        finalOutFileSize);
    Path finalIndexFile = task.mapOutputFile.getOutputIndexFileForWrite(mapId,
        finalIndexFileSize);

    // The output stream for the final single output file
    FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

    if (numSpills == 0) {
      // create dummy files
      IndexRecord rec = new IndexRecord();
      SpillRecord sr = new SpillRecord(partitions);
      try {
        for (int i = 0; i < partitions; i++) {
          long segmentStart = finalOut.getPos();
          Writer<K, V> writer = new Writer<K, V>(job, finalOut, keyClass,
              valClass, codec, null);
          writer.close();
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          sr.putIndex(rec, i);
        }
        sr.writeToFile(finalIndexFile, job);
      } finally {
        finalOut.close();
      }
      return;
    }
    {
      IndexRecord rec = new IndexRecord();
      final SpillRecord spillRec = new SpillRecord(partitions);
      for (int parts = 0; parts < partitions; parts++) {
        // create the segments to be merged
        List<Segment<K, V>> segmentList = new ArrayList<Segment<K, V>>(
            numSpills + this.inMemorySegments.length);
        for (int i = 0; i < numSpills; i++) {
          IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);
          Segment<K, V> s = new Segment<K, V>(job, rfs, filename[i],
              indexRecord.startOffset, indexRecord.partLength, codec, true);
          segmentList.add(i, s);
          if (LOG.isDebugEnabled()) {
            LOG.debug("MapId=" + mapId + " Reducer=" + parts + "Spill =" + i
                + "(" + indexRecord.startOffset + "," + indexRecord.rawLength
                + ", " + indexRecord.partLength + ")");
          }
        }
        
        if(this.inMemorySegments[parts] != null) {
          // add the in memory spill to the end of segmentList
          segmentList.add(numSpills, this.inMemorySegments[parts]);
        }
        
        // merge
        RawKeyValueIterator kvIter =
            Merger.merge(job, rfs, keyClass, valClass, codec,
                segmentList, job.getInt("io.sort.factor", 100),
                new Path(mapId.toString()), new RawComparator<K>() {
                  @Override
                  public int compare(byte[] b1, int s1, int l1,
                      byte[] b2, int s2, int l2) {
                    return LexicographicalComparerHolder.compareBytes(
                            b1, 
                            s1 + WritableUtils.INT_LENGTH_BYTES, 
                            l1 - WritableUtils.INT_LENGTH_BYTES, 
                            b2, 
                            s2 + WritableUtils.INT_LENGTH_BYTES, 
                            l2 - WritableUtils.INT_LENGTH_BYTES);
                  }

                  @Override
                  public int compare(K o1, K o2) {
                    return LexicographicalComparerHolder.compareBytes(
                    		o1.getBytes(), 0, o1.getLength(), 
                            o2.getBytes(), 0, o2.getLength());
                  }
                },  reporter, null,
                task.spilledRecordsCounter);

        // write merged output to disk
        long segmentStart = finalOut.getPos();
        Writer<K, V> writer = new Writer<K, V>(job, finalOut, keyClass,
            valClass, codec, task.spilledRecordsCounter);
        Merger.writeFile(kvIter, writer, reporter, job);
        // close
        writer.close();
        // record offsets
        rec.startOffset = segmentStart;
        rec.rawLength = writer.getRawLength();
        rec.partLength = writer.getCompressedLength();
        spillRec.putIndex(rec, parts);
      }
      spillRec.writeToFile(finalIndexFile, job);
      finalOut.close();
      for (int i = 0; i < numSpills; i++) {
        rfs.delete(filename[i], true);
      }
    }
  }

  public void close() {
    this.mapSpillSortCounter.finalCounterUpdate();
    if(numBigRecordsSpills > numBigRecordsWarnThreshold) {
      LOG.warn("Spilled a large number of big records: "
          + numBigRecordsSpills);
    }
  }
}
