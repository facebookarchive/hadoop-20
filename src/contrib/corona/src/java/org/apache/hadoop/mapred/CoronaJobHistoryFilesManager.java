package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobHistory.MovedFileInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * JobHistoryFilesManager used in CoronaJobHistory to handle changin job id on
 * remote JT restarting.
 */
@SuppressWarnings("deprecation")
public class CoronaJobHistoryFilesManager extends
    JobHistory.JobHistoryFilesManager {

  /** Logger */
  private static final Log LOG = LogFactory
      .getLog(CoronaJobHistoryFilesManager.class);
  /** Suffix for xml conf file (append to job history file name) */
  public static final String XML_CONF_SUFFIX = "_conf.xml";

  /**
   * Returns conf file filename for given job id
   * @param jobId a job id
   * @return filename
   */
  public static String getConfFilename(JobID jobId) {
    return jobId.toString() + XML_CONF_SUFFIX;
  }

  /**
   * Returns job file filename for given job id
   * @param jobId a job id
   * @return filename
   */
  public static String getHistoryFilename(JobID jobId) {
    return jobId.toString();
  }

  /**
   * C'tor
   * @param conf a configuration to use
   * @param jobTracker observer for created history
   * @param logDir where to place logs
   * @throws IOException
   */
  public CoronaJobHistoryFilesManager(Configuration conf,
      JobHistoryObserver jobTracker, Path logDir) throws IOException {
    super(conf, jobTracker, logDir);
  }

  /**
   * Moves job history and job conf files to done location for given job
   * @param id job, which files are to be moved
   * @param sync whether move files synchronously or in detached thread
   * @param renameJobId files will be moved to location of this job
   */
  void moveToDone(final JobID id, boolean sync, final JobID renameJobId) {
    // If restarting is disabled use original implementation
    if (conf.getInt(CoronaJobTracker.MAX_JT_FAILURES_CONF,
        CoronaJobTracker.MAX_JT_FAILURES_DEFAULT) == 0) {
      moveToDone(id, sync);
      return;
    }
    // Otherwise use corona-specific implementation
    final List<Path> paths = new ArrayList<Path>();
    final Path historyFile = getHistoryFile(id);
    if (historyFile == null) {
      LOG.info("No file for job-history with " + id + " found in cache!");
    } else {
      paths.add(historyFile);
    }

    final Path confPath = getConfFileWriters(id);
    if (confPath == null) {
      LOG.info("No file for jobconf with " + id + " found in cache!");
    } else {
      paths.add(confPath);
    }

    Runnable r = new Runnable() {

      public void run() {
        // move the files to doneDir folder
        try {
          List<PrintWriter> writers = getWriters(id);
          synchronized (writers) {
            if (writers.size() > 0) {
              // try to wait writers for 10 minutes 
              writers.wait(600000L);
            }
            
            if (writers.size() > 0) {
              LOG.warn("Failed to wait for writers to finish in 10 minutes.");
            }
          }

          URI srcURI = logFs.getUri();
          URI doneURI = doneFs.getUri();
          boolean useRename = (srcURI.compareTo(doneURI) == 0);

          for (Path path : paths) {
            // check if path exists, in case of retries it may not exist
            if (logFs.exists(path)) {
              LOG.info("Moving " + path.toString() + " to " +
                  doneDir.toString());

              Path dstPath;
              if (renameJobId != null) {
                if (JobHistory.CONF_FILTER.accept(path)) {
                  dstPath = new Path(doneDir, getConfFilename(renameJobId));
                } else {
                  dstPath = new Path(doneDir, getHistoryFilename(renameJobId));
                }
              } else {
                dstPath = new Path(doneDir, path.getName());
              }

              if (useRename) {
                // In the job tracker failover case, the previous job tracker may have
                // generated the history file, remove it if existed
                if (doneFs.exists(dstPath)) {
                  LOG.info("Delete the previous job tracker generaged job history file in " +
                      dstPath);
                  doneFs.delete(dstPath, true);
                }
                doneFs.rename(path, dstPath);
              } else {
                FileUtil.copy(logFs, path, doneFs, dstPath, true, true, conf);
              }

              doneFs.setPermission(dstPath, new FsPermission(
                  CoronaJobHistory.HISTORY_FILE_PERMISSION));
            }
          }
        } catch (Throwable e) {
          LOG.error("Unable to move history file to DONE folder:\n" +
              StringUtils.stringifyException(e));
        }

        String historyFileDonePath = null;
        if (historyFile != null) {
          historyFileDonePath = new Path(doneDir, historyFile.getName())
              .toString();
        }

        JobHistory.jobHistoryFileMap.put(id, new MovedFileInfo(
            historyFileDonePath, System.currentTimeMillis()));
        jobTracker.historyFileCopied(id, historyFileDonePath);

        // purge the job from the cache
        purgeJob(id);
      }

    };

    if (sync) {
      r.run();
    } else {
      executor.execute(r);
    }
  }

}
