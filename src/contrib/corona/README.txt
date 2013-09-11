Overview
---------
Hadoop Corona is the next version of Map-Reduce. The current Map-Reduce has a single Job Tracker that reached its limits at Facebook. The Job Tracker manages the cluster resource and tracks the state of each job. In Hadoop Corona, the cluster resources are tracked by a central Cluster Manager. Each job gets its own Corona Job Tracker which tracks just that one job. The design provides some key improvements:

- Scalability - The Cluster Manager tracks a small amount of information per job, and the individual Corona Job Trackers do the tracking of the tasks. This provides much better scalability with the number and size of jobs, and removes the need for Admission Control.
- Latency - task scheduling works in push model. A Corona Job Tracker pushes resource requests to the Cluster Manager and the Cluster Manager pushes resource grants back to the Corona Job Tracker. After receiving resource grants, the Corona Job Tracker pushes tasks to the Corona Task Tracker. This is contrast to the current Map-Reduce, where such scheduling decisions happen when heartbeats are received. The latency associated with the heartbeat model becomes important for small jobs.
- Fairness - generally Fair Scheduler in Corona does a better job of allocating fair shares of the resources to the pools when compared to Map-Reduce v1.
- Cluster Utilization - because of a lower scheduling overhead Corona does a better job of supplying Task Trackers with work. This way the cluster is more heavily utilized.

Understanding Corona
--------------------
A Corona Map-Reduce cluster consists of the following components:

Cluster Manager: There is only one Cluster Manager per cluster. It is responsible for allocating slots to different Jobs (using the Fair Scheduler). The Cluster Manager only keeps track of the utilization of different machines in the cluster and the assignment of compute resources to different Jobs. It's not responsible for actually running the jobs. The Cluster Manager is agnostic to map-reduce. It can (in the future) be used to schedule compute resources for any parallel computing framework
Task Trackers: This is same as Hadoop Classic. All TT's communicate with the Cluster Manager to report available compute resources. They also communicate with the Job Trackers to actually run Map-Reduce Tasks.
Corona Job Tracker: The job tracking functionality is implemented by this. It can run in two different modes: as a part of the client running the job, or as a task on one of the Task Trackers in the cluster. The first approach gives small jobs better latencies, the second approach is better for larger jobs to minimize the amount of heartbeat traffic going in and out of the cluster.
Proxy Job Tracker: The job details page for a job is served by the Corona Job Tracker while it runs. When the job finishes the Corona Job Tracker shuts down so we need another server to show the job details. To make this seamless, the Job URL always points to a Proxy Job Tracker. While the job is running, the proxy redirects to the Corona Job Tracker. When the job is done, a file is written to HDFS, and the Proxy Job Tracker reads this file to get the job details. Additionally the Proxy Job Tracker also stores and reports all of the job metrics aggregated in the cluster.
