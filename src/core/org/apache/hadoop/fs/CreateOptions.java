package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.util.Progressable;

public abstract class CreateOptions {
  
  public abstract Object getValue();

  public static BlockSize blockSize(long bs) {
    return new BlockSize(bs);
  }

  public static BufferSize bufferSize(int bs) {
    return new BufferSize(bs);
  }

  public static ReplicationFactor replicationFactor(short rf) {
    return new ReplicationFactor(rf);
  }

  public static BytesPerChecksum bytesPerChecksum(int crc) {
    return new BytesPerChecksum(crc);
  }

  public static Perms perms(FsPermission perm) {
    return new Perms(perm);
  }
  
  public static Progress progress(Progressable progress){
    return new Progress(progress);
  }
  
  /**
   * Creates a WriteOptions from given overwrite and forceSync values. If null passed,
   * it will use their default value.
   */
  public static WriteOptions writeOptions(Boolean overwrite, Boolean forceSync){
    WriteOptions wo = new WriteOptions();
    if (overwrite != null)
      wo.setOverwrite(overwrite);
    if (forceSync != null)
      wo.setForcesync(forceSync);
    return wo;
  }

  public static class BlockSize extends CreateOptions {
    private final long blockSize;

    protected BlockSize(long bs) {
      if (bs <= 0) {
        throw new IllegalArgumentException("Block size must be greater than 0");
      }
      blockSize = bs;
    }

    public Long getValue() {
      return blockSize;
    }
  }

  public static class ReplicationFactor extends CreateOptions {
    private final short replication;

    protected ReplicationFactor(short rf) {
      if (rf <= 0) {
        throw new IllegalArgumentException("Replication must be greater than 0");
      }
      replication = rf;
    }

    public Short getValue() {
      return replication;
    }
  }

  public static class BufferSize extends CreateOptions {
    private final int bufferSize;

    protected BufferSize(int bs) {
      if (bs <= 0) {
        throw new IllegalArgumentException("Buffer size must be greater than 0");
      }
      bufferSize = bs;
    }

    public Integer getValue() {
      return bufferSize;
    }
  }

  public static class BytesPerChecksum extends CreateOptions {
    private final int bytesPerChecksum;

    protected BytesPerChecksum(int bpc) {
      if (bpc <= 0) {
        throw new IllegalArgumentException("Bytes per checksum must be greater than 0");
      }
      bytesPerChecksum = bpc;
    }

    public Integer getValue() {
      return bytesPerChecksum;
    }
  }

  public static class Perms extends CreateOptions {
    private final FsPermission permissions;

    protected Perms(FsPermission perm) {
      if (perm == null) {
        throw new IllegalArgumentException("Permissions must not be null");
      }
      permissions = perm;
    }

    public FsPermission getValue() {
      return permissions;
    }
  }

  public static class Progress extends CreateOptions {
    private final Progressable progress;

    protected Progress(Progressable prog) {
      progress = prog;
    }

    public Progressable getValue() {
      return progress;
    }
  }

  /**
   * Get an option of desired type
   * 
   * @param theClass
   *          is the desired class of the opt
   * @param opts
   *          - not null - at least one opt must be passed
   * @return an opt from one of the opts of type theClass.
   *         returns null if there isn't any
   */
  public static CreateOptions getOpt(Class<? extends CreateOptions> theClass, CreateOptions... opts) {
    if (opts == null)
      return null;
    CreateOptions result = null;
    for (int i = 0; i < opts.length; ++i) {
      if (opts[i].getClass() == theClass) {
        if (result != null)
          throw new IllegalArgumentException("Multiple args with type " + theClass);
        result = opts[i];
      }
    }
    return result;
  }

  /**
   * set an option
   * 
   * @param newValue
   *          the option to be set
   * @param opts
   *          - the option is set into this array of opts
   * @return updated CreateOpts[] == opts + newValue
   * @throws IllegalArgumentException If two options with the same type as newValue exists change will apply to one of them, and an Exception will be thrown.
   */
  public static <T extends CreateOptions> CreateOptions[] setOpt(T newValue, CreateOptions ... opts) {
    boolean alreadyInOpts = false;
    if (opts != null) {
      for (int i = 0; i < opts.length; ++i) {
        if (opts[i].getClass() == newValue.getClass()) {
          if (alreadyInOpts)
            throw new IllegalArgumentException("Multiple args with type " + newValue.getClass());
          alreadyInOpts = true;
          opts[i] = newValue;
        }
      }
    }
    CreateOptions[] resultOpt = opts;
    if (!alreadyInOpts) { // no newValue in opt
      CreateOptions[] newOpts = new CreateOptions[opts.length + 1];
      System.arraycopy(opts, 0, newOpts, 0, opts.length);
      newOpts[opts.length] = newValue;
      resultOpt = newOpts;
    }
    return resultOpt;
  }
}
