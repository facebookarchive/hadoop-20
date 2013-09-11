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
package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * A place for some static methods used by the Raid unit test
 */
public class Utils {

  public static final Log LOG = LogFactory.getLog(Utils.class);

  /**
   * Load typical codecs for unit test use
   */
  public static void loadTestCodecs(Configuration conf) throws IOException {
    Utils.loadTestCodecs(conf, 5, 1, 3, "/raid", "/raidrs");
  }
  
  public static void loadTestCodecs(Configuration conf,
      int stripeLength, int xorParityLength, int rsParityLength,
      String xorParityDirectory, String rsParityDirectory)
          throws IOException {
    Utils.loadTestCodecs(conf, stripeLength, stripeLength, xorParityLength,
        rsParityLength, xorParityDirectory, rsParityDirectory, false, false);
  }
  
  public static void loadTestCodecs(Configuration conf,
      int xorStripeLength, int rsStripeLength, int xorParityLength,
      int rsParityLength, String xorParityDirectory, String rsParityDirectory)
          throws IOException {
    loadTestCodecs(conf, xorStripeLength, rsStripeLength, xorParityLength,
        rsParityLength, xorParityDirectory, rsParityDirectory, 
        false, false);
  }
  
  public static void loadTestCodecs(Configuration conf,
      int xorStripeLength, int rsStripeLength, int xorParityLength,
      int rsParityLength, String xorParityDirectory, String rsParityDirectory,
      boolean isSimulatedBlockFixed, boolean isDirRaid)
          throws IOException {
    loadTestCodecs(conf, xorStripeLength, rsStripeLength, xorParityLength,
        rsParityLength, xorParityDirectory, rsParityDirectory, 
        isSimulatedBlockFixed, "org.apache.hadoop.raid.XORCode",
        "org.apache.hadoop.raid.ReedSolomonCode",
        isDirRaid);
  }

  /**
   * Load RS and XOR codecs with given parameters
   */
  public static void loadTestCodecs(Configuration conf,
      int xorStripeLength, int rsStripeLength, int xorParityLength,
      int rsParityLength, String xorParityDirectory, String rsParityDirectory,
      boolean isSimulatedBlockFixed, String xorCode, String rsCode, 
      boolean isDirRaid)
          throws IOException {
    Builder[] builders = new Builder[]{
    getXORBuilder().setParityDir(xorParityDirectory).setStripeLength(
        xorStripeLength).setParityLength(xorParityLength).setCodeClass(
        xorCode).simulatedBlockFixed(isSimulatedBlockFixed).dirRaid(
        isDirRaid), 
    getRSBuilder().setParityDir(rsParityDirectory).setStripeLength(
        rsStripeLength).setParityLength(rsParityLength).setCodeClass(
        rsCode).simulatedBlockFixed(isSimulatedBlockFixed).dirRaid(
        isDirRaid)}; 
    loadTestCodecs(conf, builders);
  }
  
  public static void loadAllTestCodecs(Configuration conf,
      int stripeLength, int xorParityLength, int rsParityLength,
      String xorParityDirectory, String rsParityDirectory,
      String dirXorParityDirectory, String dirRsParityDirectory) 
          throws IOException {
    Builder[] builders = new Builder[] {
        getXORBuilder().setStripeLength(stripeLength).setParityLength(
            xorParityLength).setParityDir(xorParityDirectory),
        getRSBuilder().setStripeLength(stripeLength).setParityLength(
            rsParityLength).setParityDir(rsParityDirectory),
        getDirXORBuilder().setStripeLength(stripeLength).setParityLength(
            xorParityLength).setParityDir(dirXorParityDirectory),
        getDirRSBuilder().setStripeLength(stripeLength).setParityLength(
            rsParityLength).setParityDir(dirRsParityDirectory)
    };
    loadTestCodecs(conf, builders);
  }
  
  public static void loadTestCodecs(Configuration conf, Builder[] codecs) 
    throws IOException {
    Codec.clearCodecs();
    StringBuilder sb = new StringBuilder();
    sb.append("[ ");
    for (Builder codec : codecs) {
      sb.append(codec.getCodecJson());
    }
    sb.append(" ] ");
    LOG.info("raid.codecs.json=" + sb.toString());
    conf.set("raid.codecs.json", sb.toString());
    Codec.initializeCodecs(conf);
    LOG.info("Test codec loaded");
    for (Codec c : Codec.getCodecs()) {
      LOG.info("Loaded raid code:" + c.id);
    }
    RaidNodeMetrics.clearInstances();
  }
  
  public static Builder getXORBuilder() {
    return (new Utils()).new Builder("xor", "/raid", 5, 1, 100,
        "org.apache.hadoop.raid.XORCode",
        false, false);
  }
  
  public static Builder getDirXORBuilder() {
    return (new Utils()).new Builder("dir-xor", "/dir-raid", 5, 1, 400,
        "org.apache.hadoop.raid.XORCode",
        false, true);
  }
  
  public static Builder getRSBuilder() {
    return (new Utils()).new Builder("rs", "/raidrs", 5, 3, 300,
        "org.apache.hadoop.raid.ReedSolomonCode",
        false, false);
  }
  
  public static Builder getDirRSBuilder() {
    return (new Utils()).new Builder("dir-rs", "/dir-raidrs", 5, 3, 600,
        "org.apache.hadoop.raid.ReedSolomonCode",
        false, true);
  }
  
  public class Builder {
    private String id = "xor";
    private String parityDirectory = "/raid";
    private long stripeLength = 5;
    private int parityLength = 1;
    private int priority = 100;
    private String codeClass = "org.apache.hadoop.raid.XORCode";
    private boolean isDirRaid = false;
    private boolean isSimulatedBlockFixed = false;
    
    public Builder(String id, String parityDirectory,
        int stripeLength, int parityLength, int priority, 
        String codeClass, boolean isSimulatedBlockFixed, boolean isDirRaid) {
      this.id = id;
      this.parityDirectory = parityDirectory;
      this.stripeLength = stripeLength;
      this.parityLength = parityLength;
      this.priority = priority;
      this.codeClass = codeClass;
      this.isDirRaid = isDirRaid;
      this.isSimulatedBlockFixed = isSimulatedBlockFixed;
    }
    
    public Builder setCodeId(String newId) {
      this.id = newId;
      return this;
    }
    
    public Builder setParityDir(String newParityDirectory) {
      this.parityDirectory = newParityDirectory;
      return this;
    }
    
    public Builder setStripeLength(long newStripeLength) {
      this.stripeLength = newStripeLength;
      return this;
    }
    
    public Builder setParityLength(int newParityLength) {
      this.parityLength = newParityLength;
      return this;
    }
    
    public Builder setPriority(int newPriority) {
      this.priority = newPriority;
      return this;
    }
    
    public Builder setCodeClass(String newCodeClass) {
      this.codeClass = newCodeClass;
      return this;
    }
    
    public Builder dirRaid(boolean isDirRaid) {
      this.isDirRaid = isDirRaid;
      return this;
    }
    
    public Builder simulatedBlockFixed(boolean isSimulatedBlockFixed) {
      this.isSimulatedBlockFixed = isSimulatedBlockFixed;
      return this;
    }
    
    public String getCodecJson() {
      return
          " { " +
            "\"id\":\"" + id  + "\"," +
            "\"parity_dir\":\"" + parityDirectory + "\"," +
            "\"tmp_parity_dir\":\"" + "/tmp" + parityDirectory + "\"," +
            "\"tmp_har_dir\":\"" + "/tmp" + parityDirectory + "_har" + "\"," +
            "\"stripe_length\":" + stripeLength + "," +
            "\"parity_length\":" + parityLength + "," +
            "\"priority\":" + priority + "," +
            "\"erasure_code\":\"" + codeClass + "\"," +
            "\"description\":\"" + id + " Code\"," +
            "\"dir_raid\":" + isDirRaid + "," +
            "\"simulate_block_fix\":" + isSimulatedBlockFixed + "," +
          " }, ";
    }
  }

}
