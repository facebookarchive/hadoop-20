/**
 * Placed in public domain
 * http://www.qbrundage.com/michaelb/pubs/essays/random_number_generation.html
 */

package org.apache.hadoop.hashtable;

/**
 * Random number generator.
 *
 * @author tomasz
 *
 */
public final class MT {
  private int mt_index;
  private int[] mt_buffer = new int[624];

  public MT() {
    java.util.Random r = new java.util.Random();
    for (int i = 0; i < 624; i++)
      mt_buffer[i] = r.nextInt();
    mt_index = 0;
  }

  public int random() {
    if (mt_index == 624) {
      mt_index = 0;
      int i = 0;
      int s;
      for (; i < 624 - 397; i++) {
        s = (mt_buffer[i] & 0x80000000) | (mt_buffer[i + 1] & 0x7FFFFFFF);
        mt_buffer[i] = mt_buffer[i + 397] ^ (s >> 1) ^ ((s & 1) * 0x9908B0DF);
      }
      for (; i < 623; i++) {
        s = (mt_buffer[i] & 0x80000000) | (mt_buffer[i + 1] & 0x7FFFFFFF);
        mt_buffer[i] = mt_buffer[i - (624 - 397)] ^ (s >> 1)
            ^ ((s & 1) * 0x9908B0DF);
      }

      s = (mt_buffer[623] & 0x80000000) | (mt_buffer[0] & 0x7FFFFFFF);
      mt_buffer[623] = mt_buffer[396] ^ (s >> 1) ^ ((s & 1) * 0x9908B0DF);
    }
    return mt_buffer[mt_index++];
  }
}
