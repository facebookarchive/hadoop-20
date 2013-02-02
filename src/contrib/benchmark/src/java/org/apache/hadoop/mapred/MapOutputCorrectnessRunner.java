package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * This can be used to run repeated instances of {@link MapOutputCorrectness}
 * until a failure is reached.  It is primarily intended to be used with
 * random parameters (-M, -R, -K, -V) in order to generate many different
 * test cases.
 */
public class MapOutputCorrectnessRunner {
  public static void main(String[] args) throws Exception{
    int ret = 0;
    while (ret == 0) {
      ret = ToolRunner.run(
          new Configuration(), new MapOutputCorrectness(), args);
    }
  }
}
