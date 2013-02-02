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

package org.apache.hadoop.hashtable;

import java.io.PrintStream;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * LightWeightGSets intstance for storing Long objects.
 * @author tomasz
 *
 */

public class LightWeightGSet implements LightWeightSet {

  static final int MAX_ARRAY_LENGTH = 1 << 30; // prevent int overflow problem
  static final int MIN_ARRAY_LENGTH = 1;

  private final LongInfo[] entries;
  private final int hash_mask;
  private int size = 0;
  private volatile int modification = 0;

  public LightWeightGSet(final int recommended_length) {
    final int actual = actualArrayLength(recommended_length);
    entries = new LongInfo[actual];
    hash_mask = entries.length - 1;
  }

  // compute actual length
  private static int actualArrayLength(int recommended) {
    if (recommended > MAX_ARRAY_LENGTH) {
      return MAX_ARRAY_LENGTH;
    } else if (recommended < MIN_ARRAY_LENGTH) {
      return MIN_ARRAY_LENGTH;
    } else {
      final int a = Integer.highestOneBit(recommended);
      return a == recommended ? a : a << 1;
    }
  }

  public int size() {
    return size;
  }

  private int getIndex(final LongInfo key) {
    return key.hashCode() & hash_mask;
  }

  private LongInfo convert(final LongInfo e) {
    @SuppressWarnings("unchecked")
    final LongInfo r = (LongInfo) e;
    return r;
  }

  public LongInfo get(final LongInfo key) {
    // validate key
    if (key == null) {
      throw new NullPointerException("key == null");
    }
    // find element
    final int index = getIndex(key);
    for (LongInfo e = entries[index]; e != null; e = e.getNext()) {
      if (e.equals(key)) {
        return convert(e);
      }
    }
    // element not found
    return null;
  }

  public boolean contains(final LongInfo key) {
    return get(key) != null;
  }

  public LongInfo put(final LongInfo element) {
    // validate element
    if (element == null) {
      throw new NullPointerException("Null element is not supported.");
    }

    if (!(element instanceof LongInfo)) {
      throw new IllegalArgumentException(
          "!(element instanceof LinkedElement), element.getClass()="
              + element.getClass());
    }
    final LongInfo e = (LongInfo) element;

    // find index
    final int index = getIndex(element);

    // remove if it already exists
    final LongInfo existing = remove(index, element);

    // insert the element to the head of the linked list
    modification++;
    size++;
    e.setNext(entries[index]);
    entries[index] = e;

    return existing;
  }

  private LongInfo remove(final int index, final LongInfo key) {
    if (entries[index] == null) {
      return null;
    } else if (entries[index].equals(key)) {
      // remove the head of the linked list
      modification++;
      size--;
      final LongInfo e = entries[index];
      entries[index] = e.getNext();
      e.setNext(null);
      return convert(e);
    } else {
      // head != null and key is not equal to head
      // search the element
      LongInfo prev = entries[index];
      for (LongInfo curr = prev.getNext(); curr != null;) {
        if (curr.equals(key)) {
          // found the element, remove it
          modification++;
          size--;
          prev.setNext(curr.getNext());
          curr.setNext(null);
          return convert(curr);
        } else {
          prev = curr;
          curr = curr.getNext();
        }
      }
      // element not found
      return null;
    }
  }

  public LongInfo remove(final LongInfo key) {
    // validate key
    if (key == null) {
      throw new NullPointerException("key == null");
    }
    return remove(getIndex(key), key);
  }

  public Iterator<LongInfo> iterator() {
    return new SetIterator();
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName());
    b.append("(size=").append(size).append(String.format(", %08x", hash_mask))
        .append(", modification=").append(modification)
        .append(", entries.length=").append(entries.length).append(")");
    return b.toString();
  }

  /** Print detailed information of this object. */
  public void printDetails(final PrintStream out) {
    out.print(this + ", entries = [");
    for (int i = 0; i < entries.length; i++) {
      if (entries[i] != null) {
        LongInfo e = entries[i];
        out.print("\n  " + i + ": " + e);
        for (e = e.getNext(); e != null; e = e.getNext()) {
          out.print(" -> " + e);
        }
      }
    }
    out.println("\n]");
  }

  private class SetIterator implements Iterator<LongInfo> {
    /** The starting modification for fail-fast. */
    private final int startModification = modification;
    /** The current index of the entry array. */
    private int index = -1;
    /** The next element to return. */
    private LongInfo next = nextNonemptyEntry();

    /** Find the next nonempty entry starting at (index + 1). */
    private LongInfo nextNonemptyEntry() {
      for (index++; index < entries.length && entries[index] == null; index++)
        ;
      return index < entries.length ? entries[index] : null;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public LongInfo next() {
      if (modification != startModification) {
        throw new ConcurrentModificationException("modification="
            + modification + " != startModification = " + startModification);
      }

      final LongInfo e = convert(next);

      // find the next element
      final LongInfo n = next.getNext();
      next = n != null ? n : nextNonemptyEntry();

      return e;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported.");
    }
  }
}
