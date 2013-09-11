package org.apache.hadoop.hdfs.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestWriteBlockHeader {

  @Test
  public void testSerializeFadvise() throws Exception {
    for (int fadvise = 0; fadvise < 6; fadvise++) {
      // Build header
      WriteBlockHeader header = getHeader();
      header.getWritePipelineInfo().getWriteOptions().setFadvise(fadvise);

      // Write out header.
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      header.write(new DataOutputStream(out));
      byte[] buffer = out.toByteArray();

      // Read and verify.
      ByteArrayInputStream in = new ByteArrayInputStream(buffer);
      WriteBlockHeader actualHeader = new WriteBlockHeader(getVersion());
      actualHeader.readFields(new DataInputStream(in));
      assertEquals(fadvise, actualHeader.getWritePipelineInfo()
          .getWriteOptions().getFadvise());
    }
  }

  private VersionAndOpcode getVersion() {
    return new VersionAndOpcode(DataTransferProtocol.DATA_TRANSFER_VERSION,
        (byte) 1);
  }

  private WriteBlockHeader getHeader() {
    VersionAndOpcode v = getVersion();
    WriteBlockHeader header = new WriteBlockHeader(v);
    header.set(1, 1, 1, 1, false, false, null, 0, null, "test");
    return header;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAdvice() {
    WriteBlockHeader header = getHeader();
    header.getWritePipelineInfo().getWriteOptions().setFadvise(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAdvice1() {
    WriteBlockHeader header = getHeader();
    header.getWritePipelineInfo().getWriteOptions().setFadvise(6);
  }
}
