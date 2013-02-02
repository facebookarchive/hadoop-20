package org.apache.hadoop.raid;

import java.io.File;

import org.junit.Test;

import junit.framework.TestCase;

public class TestBlockFixerWithChecksumStore extends TestCase {
  TestBlockFixer tbf = new TestBlockFixer();
  @Test
  public void testBlockFixDist() throws Exception {
    tbf.implBlockFix(false, true);
  }

  @Test
  public void testBlockFixLocal() throws Exception {
    tbf.implBlockFix(true, true);
  }
  
  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedBlockDist() throws Exception {
    tbf.generatedBlockTestCommon("testGeneratedBlock", 3, false, true);
  }

  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedBlockLocal() throws Exception {
    tbf.generatedBlockTestCommon("testGeneratedBlock", 3, true, true);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockDist() throws Exception {
    tbf.generatedBlockTestCommon("testGeneratedLastBlock", 6, false, true);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockLocal() throws Exception {
    tbf.generatedBlockTestCommon("testGeneratedLastBlock", 6, true, true);
  }

  @Test
  public void testParityBlockFixDist() throws Exception {
    tbf.implParityBlockFix("testParityBlockFixDist", false, true);
  }

  @Test
  public void testParityBlockFixLocal() throws Exception {
    tbf.implParityBlockFix("testParityBlockFixLocal", true, true);
  }
}
