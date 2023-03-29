This project contains the source code for reproducing the experimental results of Flink in our SIGMOD paper 
*"Generalizing Bulk-Synchronous Parallel Processing: From Data to Threads and Agent-Based Simulations"*.

### Workloads
The workloads contain three simulation applications: population dynamics (gameOfLife), economics (stockMarket), and epidemics. 
The source code for implementing each of these simulations in Flink as a vertex program can be found in `/src/main/scala/simulations/`.

### Benchmark
Our paper included the following benchmark experiments in Flink: tuning, scaleup, scaleout, communication frequency, and computation interval. For each of the benchmark, you can find its corresponding configuration in `conf/`, which is necessary for launching a benchmark. The input graphs are not included here due to their sheer size, but you should be able to easily generate them based on our description in the paper.

The driver script for starting a benchmark is `/bin/bench.py`. Prior to running a benchmark, you need to compile and assemble the vertex programs. Our benchmark script automates this for you by passing `-a`. In short, to compile and assemble the vertex program and then running a benchmark named {test}, you should enter the following command (tested with python3.9, but you can easily adjust the driver script to use other versions of Python):

```python3 bin/bench.py -a -t test```.

### Remark
Before running a benchmark, you should already have the Flink cluster up and running. Our experiments are done using Flink 1.16.1, tested on CentOS 7 using a cluster of Xeon servers. Each server has 24 cores and 220 GB RAM. For the best performance, you need to tune the configuration of Flink as specified in `conf/flink-conf.yaml` (this is *not* part of our benchmark configuration.) In our experiments, the most speedup was observed when increasing the total process memory size for the task manager (`taskmanager.memory.process.size: 172800m`) and the number of task slots `taskmanager.numberOfTaskSlots: 100`. 