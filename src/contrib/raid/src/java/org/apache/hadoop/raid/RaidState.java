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
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.raid.ExpandedPolicy.ExpandedPolicyComparator;
import org.apache.hadoop.raid.protocol.PolicyInfo;

/**
 * The state of a source {@link FileStatus} under a given {@link PolicyInfo}
 */
public enum RaidState {
  RAIDED,
  NOT_RAIDED_TOO_NEW,
  NOT_RAIDED_TOO_SMALL,
  NOT_RAIDED_BUT_SHOULD,
  NOT_RAIDED_OTHER_POLICY,
  NOT_RAIDED_NO_POLICY;

  public static final int TOO_SMALL_NOT_RAID_NUM_BLOCKS = 2;
  public static final long ONE_DAY_MSEC = 86400 * 1000;
  public static final Log LOG = LogFactory.getLog(RaidState.class);
  static final String TRASH_PATTERN = ".Trash";

  final static private ExpandedPolicyComparator expandedPolicyComparator =
      new ExpandedPolicyComparator();

  public static class ThreadLocalDateFormat {
    private final String format;

    /**
     * Constructs {@link ThreadLocalDateFormat} using given date format pattern
     * @param format Date format pattern
     */
    public ThreadLocalDateFormat(String format) {
      this.format = format;
    }

    /**
     * ThreadLocal based {@link SimpleDateFormat}
     */
    private final ThreadLocal<SimpleDateFormat> dateFormat = 
      new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat df = new SimpleDateFormat(format);
          return df;
        }
      };

    SimpleDateFormat get() {
      return dateFormat.get();
    }
  }

  public static class Checker {

    final private Configuration conf;
    final private List<ExpandedPolicy> sortedExpendedPolicy;
    private boolean inferMTimeFromName;
    public static final ThreadLocalDateFormat dateFormat =
      new ThreadLocalDateFormat("yyyy-MM-dd");
    private List<String> excludePatterns = new ArrayList<String>();
    private List<FileStatus> lfs = null;

    public Checker(Collection<PolicyInfo> allInfos, Configuration conf)
        throws IOException {
      this.conf = conf;
      List<ExpandedPolicy> sortedExpendedPolicy =
            new ArrayList<ExpandedPolicy>();
      for (PolicyInfo policy : allInfos) {
        sortedExpendedPolicy.addAll(ExpandedPolicy.expandPolicy(policy));
      }
      Collections.sort(sortedExpendedPolicy, expandedPolicyComparator);
      this.sortedExpendedPolicy =
        Collections.unmodifiableList(sortedExpendedPolicy);
      this.inferMTimeFromName = conf.getBoolean("raid.infermtimefromname", true);
      excludePatterns.add(TRASH_PATTERN);
      String excluded = conf.get("raid.exclude.patterns");
      if (excluded != null) {
        for (String p: excluded.split(",")) {
          excludePatterns.add(p);
        }
      }
    }
    
    public RaidState check(PolicyInfo info, FileStatus file, long now,
        boolean skipParityCheck) throws IOException {
      return check(info, file, now, skipParityCheck, null);
    }

    /**
     * Check the state of a raid source file against a policy
     * @param info The policy to check
     * @param file The source file to be checked
     * @param now The system millisecond time
     * @param skipParityCheck Skip checking the existence of parity. Checking
     *                        parity is very time-consuming for HAR parity file
     * @param lfs The list of FileStatus of files under the directory, only used
     *        by directory raid.
     * @return The state of the raid file
     * @throws IOException
     */
    public RaidState check(PolicyInfo info, FileStatus file, long now,
        boolean skipParityCheck, List<FileStatus> lfs) throws IOException {
      ExpandedPolicy matched = null;
      long mtime = -1;
      String uriPath = file.getPath().toUri().getPath();
      if (inferMTimeFromName) {
        mtime = mtimeFromName(uriPath);
      }
      // If we can't infer the mtime from the name, use the mtime from filesystem.
      // If the the file is newer than a day, use the mtime from filesystem.
      if (mtime == -1 ||
          Math.abs(file.getModificationTime() - now) < ONE_DAY_MSEC) {
        mtime = file.getModificationTime();
      }
      boolean hasNotRaidedButShouldPolicy = false; 
      for (ExpandedPolicy policy : sortedExpendedPolicy) {
        if (policy.parentPolicy == info) {
          matched = policy;
          break;
        }
        RaidState rs = policy.match(file, mtime, now, conf, lfs);
        if (rs == RaidState.RAIDED) {
          return NOT_RAIDED_OTHER_POLICY;
        } else if (rs == RaidState.NOT_RAIDED_BUT_SHOULD) {
          hasNotRaidedButShouldPolicy = true; 
        } 
      }
      if (matched == null) {
        return NOT_RAIDED_NO_POLICY;
      }

      // The preceding checks are more restrictive,
      // check for excluded just before parity check.
      if (shouldExclude(uriPath)) {
        return NOT_RAIDED_NO_POLICY;
      }
      
      if (file.isDir() != matched.codec.isDirRaid) {
        return NOT_RAIDED_NO_POLICY;
      }

      long blockNum = matched.codec.isDirRaid?
          DirectoryStripeReader.getBlockNum(lfs):
          computeNumBlocks(file);

      if (blockNum <= TOO_SMALL_NOT_RAID_NUM_BLOCKS) {
        return NOT_RAIDED_TOO_SMALL;
      }
      
      RaidState finalState = matched.getBasicState(file, mtime, now,
          skipParityCheck, conf, lfs);
      if (finalState == RaidState.RAIDED) {
        return finalState;
      } else if (hasNotRaidedButShouldPolicy) {
        return RaidState.NOT_RAIDED_OTHER_POLICY;
      } else {
        return finalState;
      }
    }

    private static int computeNumBlocks(FileStatus status) {
      return (int)Math.ceil(((double)(status.getLen())) / status.getBlockSize());
    }

    // HACK!
    public static long mtimeFromName(String file) {
      int dsIdx = file.indexOf("/ds=");
      if (dsIdx == -1) {
        return -1;
      }
      try {
        Date date = dateFormat.get().parse(file, new ParsePosition(dsIdx + 4));
        if (date != null) {
          return date.getTime();
        } else {
          return -1;
        }
      } catch (Exception e) {
        LOG.error("Exception in parsing " + file, e);
        return -1;
      }
    }

    public boolean shouldExclude(String file) {
      for (String exclude: excludePatterns) {
        if (file.indexOf(exclude) != -1) {
          return true;
        }
      }
      return false;
    }
  }
}
