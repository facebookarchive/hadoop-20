/**
 * Source of getHash64, getHash32, getHashRJ, getHash32ShiftMul, getHash6432shift:
 * http://www.cris.com/~Ttwang/tech/inthash.htm
 */

package org.apache.hadoop.hashtable;

/**
 * Several useful hash functions.
 *
 * @author tomasz
 *
 */
public class Hashes {

  public static int getHashJava(long key) {
    int hash = (int) (key ^ (key >>> 32));
    return hash;
  }

  public static int getHashCurrent(long key) {
    int hash = (37 * 17 + (int) (key ^ (key >>> 32)));
    return hash;
  }

  public static int getHash64(long key) {
    key = (~key) + (key << 21);
    key = key ^ (key >>> 24);
    key = (key + (key << 3)) + (key << 8);
    key = key ^ (key >>> 14);
    key = (key + (key << 2)) + (key << 4);
    key = key ^ (key >>> 28);
    key = key + (key << 31);
    return (int) key;
  }

  public static int getHash32(int key) {
    key = ~key + (key << 15);
    key = key ^ (key >>> 12);
    key = key + (key << 2);
    key = key ^ (key >>> 4);
    key = key * 2057;
    key = key ^ (key >>> 16);
    return (int) key;
  }

  public static int getHashRJ(int key) {
    key = (key + 0x7ed55d16) + (key << 12);
    key = (key ^ 0xc761c23c) ^ (key >> 19);
    key = (key + 0x165667b1) + (key << 5);
    key = (key + 0xd3a2646c) ^ (key << 9);
    key = (key + 0xfd7046c5) + (key << 3);
    key = (key ^ 0xb55a4f09) ^ (key >> 16);
    return key;
  }

  public static int getHash32ShiftMul(int key) {
    int c2 = 0x27d4eb2d;
    key = (key ^ 61) ^ (key >>> 16);
    key = key + (key << 3);
    key = key ^ (key >>> 4);
    key = key * c2;
    key = key ^ (key >>> 15);
    return (int) key;
  }

  public static int getHash6432shift(long key) {
    key = (~key) + (key << 18);
    key = key ^ (key >>> 31);
    key = key * 21;
    key = key ^ (key >>> 11);
    key = key + (key << 6);
    key = key ^ (key >>> 22);
    return (int) key;
  }

  public static int getHash(long id, int which) {
    switch (which) {
    case 0:
      return getHashJava(id);
    case 1:
      return getHashCurrent(id);
    case 2:
      return getHash64(id);
    case 3:
      return getHash32((int) id);
    case 4:
      return getHashRJ((int) id);
    case 5:
      return getHash32ShiftMul((int) id);
    case 6:
      return getHash6432shift(id);
    default:
      return 0;
    }
  }

  public static String getHashDesc(int which) {
    switch (which) {
    case 0:
      return "HashJava";
    case 1:
      return "HashCurrent";
    case 2:
      return "Hash64";
    case 3:
      return "Hash32";
    case 4:
      return "HashRJ";
    case 5:
      return "Hash32ShiftMul";
    case 6:
      return "Hash6432Shift";
    default:
      return "HashDummy";
    }
  }

}
