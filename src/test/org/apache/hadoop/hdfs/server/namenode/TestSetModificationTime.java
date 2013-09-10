package org.apache.hadoop.hdfs.server.namenode;

import java.lang.reflect.Constructor;

import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that we don't have assertion failures with setModificationTime on
 * INodeHardLinkFile.
 */
public class TestSetModificationTime {

  private static INodeHardLinkFile file;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // Enable assertions for INodeHardLinkFile
    ClassLoader loader = ClassLoader.getSystemClassLoader();
    loader.setDefaultAssertionStatus(true);
    Class<?> c = loader
        .loadClass("org.apache.hadoop.hdfs.server.namenode.INodeHardLinkFile");
    Constructor<?> ctor = c.getDeclaredConstructor(INodeFile.class, Long.TYPE);
    INodeFile inode = new INodeFile();
    inode.setLocalName("test");
    inode.setReplication((short) 1);
    file = (INodeHardLinkFile) ctor.newInstance(inode, 1);
  }

  @Test
  public void testMod() {
    file.setModificationTime(1);
    assertEquals(1, file.getModificationTime());
    file.setModificationTimeForce(0);
    assertEquals(0, file.getModificationTime());
  }
}
