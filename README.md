This project contains the source code for reproducing the experimental results of Flink in our SIGMOD paper 
*"Generalizing Bulk-Synchronous Parallel Processing: From Data to Threads and Agent-Based Simulations"*.

### Workloads
The workloads contain applications selected from areas where simulations are prevalent: population dynamics (gameOfLife), economics (stockMarket), and epidemics. 
The source code for implementing each of these simulations in Flink as a vertex program can be found in `/src/main/scala/simulations/`.

### Benchmark
Our paper included the following benchmark experiments in Flink: tuning, scaleup, scaleout, communication frequency, and computation interval. For each of the benchmark, you can find its corresponding configuration in `conf/`, which is necessary for launching a benchmark. The input graphs are not included here due to their size, but you should be able to easily generate them based on our description in the paper.

The driver script for starting a benchmark is `/bin/bench.py`. Prior to running a benchmark, the vertex programs need to be compiled and assembled to create a uber jar that will be run on the cluster. Our benchmark script automatically cleans existing jar, if any, and creates a new jar. If undesired ,you can disable this default behavior in `/bin/bench.py`. In short, to compile and assemble the vertex program and then running a benchmark named {test}, you should enter the following command (tested with python3.9, but you can easily adjust the driver script to use other versions of Python):

```python3 bin/bench.py -t test```.

As a reference, you can checkout our measured performance in `benchmark/`.

### Remark
Before running a benchmark, you should already have the Flink cluster up and running. Our experiments were done using Flink 1.16.1 on CentOS 7. We used a cluster of servers, each with 24 cores (two Xeon processors, 48 hardware threads, supporting hyper-threading), 256GB of RAM, and 400GB of SSD. To obtain the best performance, you need to tune the configuration of Flink in `conf/flink-conf.yaml` (this is defined in Flink, *not* part of our benchmark configuration.) In our experiments, the performance improvement mainly came from increasing the process memory size for the task manager in Flink (`taskmanager.memory.process.size: 172800m`) and the number of task slots `taskmanager.numberOfTaskSlots: 100`. 
