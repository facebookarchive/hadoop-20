package org.apache.hadoop.streaming;

import java.io.IOException;

import java.util.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class InmemBufferingOutputCollector implements BufferingOutputCollector {

    private LinkedList<Writable> keyList;
    private LinkedList<Writable> valList;
    private Writable [] retArray;

    public InmemBufferingOutputCollector () {
        keyList = new LinkedList<Writable> ();
        valList = new LinkedList<Writable> ();
        retArray = new Writable[2];
    }

    public void collect(Object key, Object value) {
        keyList.add((Writable) key);
        valList.add((Writable) value);
    }

    public Writable[] retrieve() {
        if(keyList.isEmpty())
            return null;

        retArray[0] = keyList.removeFirst();
        retArray[1] = valList.removeFirst();
        return (retArray);
    }
}
