Simulation workloads implemented in Flink 

You can package the application into a fat jar with `sbt assembly`, then submit it to a running Flink cluster. 

For game of life example,
```
flink run -c simulations.GameOfLife /path/to/your/project/my-app/target/scala-2.11/testme-assembly-0.1-SNAPSHOT.jar 1 /path/to/input/edgeFile
```
