#!/bin/bash

set -e

cores=("1" "10" "20" "50")
agents=("1000" "10000")
# agents=("1000" "5000" "10000" "50000" "100000")

FLINK_HOME="/root/flink-1.16.1"

logDir="log"
experiment="GameOfLife"
edgeFilePrefix="2DTorus"

CWD=`pwd`

mkdir -p ${logDir}

echo $CWD 

for total in "${agents[@]}"
do
    for core in "${cores[@]}"
    do
        log_file=${logDir}/"${experiment}_${core}cores_${total}"
        touch $log_file
        echo "Cores $core Agents $total"
        ${FLINK_HOME}/bin/flink run -c simulations.${experiment} target/scala-2.12/benchmark-assembly-0.1-SNAPSHOT.jar $cores ${CWD}/data/${edgeFilePrefix}_${total}_graphx.txt >> $log_file 2>&1
        sleep 1
        sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
    done
done

