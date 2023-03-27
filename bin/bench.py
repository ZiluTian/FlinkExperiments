# -*- coding: utf-8 -*-
import json
import subprocess
import os
import sys
from datetime import datetime

def run(cores, cfreqs, cintervals):
    # Comm. freq experiments
    for core in cores:
        for cfreq in cfreqs:
            for cint in cintervals:
                for experiment in EXPERIMENTS:
                    for input_file in config[experiment]['input_graph']:
                        print(f"Running {experiment} cores {core} communication frequency is {cfreq} computation interval is {cint}")
                        cp = config[experiment]["classpath"]
                        now = datetime.now()
                        current_time = now.strftime("%H%M%S")
                        log_file = open(f"{LOG_DIR}/{ITERATION}/{experiment}_cores{core}_cfreq{cfreq}_cint{cint}_{current_time}", 'a')
                        process = subprocess.run([f'{FLINK_HOME}/bin/flink', 'run', '-c', cp, '-Dexecution.runtime-mode=BATCH', 'target/scala-2.12/benchmark-assembly-0.1-SNAPSHOT.jar', str(core), input_file, str(cfreq), str(cint)], text=True, stdout=subprocess.PIPE, check=True)
                        print(process.stdout, file=log_file)
                        os.system('echo 3 > /proc/sys/vm/drop_caches')
                        log_file.flush()
                        log_file.close()

if (__name__ == "__main__"):
    assemble = False
    experiment = ""

    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        if arg == '-t':
            experiment = sys.argv[i+1]
        elif arg == '-a':
            assemble = True
    
    if (experiment == ""):
        print("Please input what benchmark to run")
        exit(1)

    f = open(f"bin/conf.json")
    config = json.load(f)

    EXPERIMENTS = config['experiments']
    REPEAT = config['repeat']
    LOG_DIR = config['log_dir']
    CFRES = config['cfreqs']
    CINT = config['cinterval']
    CORES = config['cores']

    FLINK_HOME=config['flink']['home']
   
    subprocess.run(['mkdir', '-p', LOG_DIR], text=True, stdout=subprocess.PIPE, check=True)
    
    if (assemble):
        subprocess.run(['sbt', 'assembly'], text=True, stdout=subprocess.PIPE, check=True)
        print(f"Assemble jar file for deployment completed")

    for i in range(REPEAT):
        ITERATION = i
        if (experiment == "cfreq"):
            run(50, CFRES, [1])
        elif (experiment == "cint"):
            run(50, [1], CINT)
        elif (experiment == "tuning"):
            run(CORES, [1], [1])
