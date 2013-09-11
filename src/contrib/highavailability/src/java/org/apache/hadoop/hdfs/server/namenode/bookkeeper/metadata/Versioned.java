package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata;

/**
 * Stores an object and its version. Since Java doesn't have the
 * equivalent to std::pair (and so far, there are no other uses within
 * the codebase for generalized pair class), this class is used to
 * store a version of a object (e.g., version from of the ZNode from
 * which the object was read) along with the object for concurrency control
 * (e.g., verifying that we do not accidentally delete an ZNode that has
 * been updated by another another process.)
 */
public class Versioned<T> {

  private final int version;
  private final T entry;

  /**
   * Store an object along side with its version
   * @param version The version of the object
   * @param entry The actual object
   */
  public Versioned(int version, T entry) {
    this.version = version;
    this.entry = entry;
  }

  /**
   * Factory method for the class. Since Java will infer generic types in
   * methods (but not in constructors), we can write:
   * <code>
   *   Versioned<Foo> fooVersioned = Versioned.of(1, foo);
   * </code>
   * instead of
   * <code>
   *   Versioned<Foo> fooVersioned = new Versioned<Foo>(1, foo);
   * </code>
   * @see #Versioned(int, Object)
   */
  public static <T> Versioned<T> of(int version, T entry) {
    return new Versioned<T>(version, entry);
  }

  public int getVersion() {
    return version;
  }

  public T getEntry() {
    return entry;
  }
}
