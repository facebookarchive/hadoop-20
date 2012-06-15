package org.apache.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;


public interface BufferingOutputCollector extends OutputCollector {

    /** Get's the next buffered key,value pair
     *
     * @return array of length 2 containing the key and value
     */
    public Writable[] retrieve() throws IOException;
}



