# -*- coding: utf-8 -*-
import json
import subprocess
import os
import sys
from datetime import datetime

# user input
MICRO_BENCHMARK = ""

def run(cores, cfreqs, cintervals):
    for core in cores:
        for cfreq in cfreqs:
            for cint in cintervals:
                for experiment in EXPERIMENTS:
                    for input_file in config[experiment]['input_graph']:
                        print(f"Running {experiment} cores {core} communication frequency is {cfreq} computation interval is {cint}")
                        cp = config[experiment]["classpath"]
                        now = datetime.now()
                        current_time = now.strftime("%H%M%S")
                        if not os.path.exists(f"{LOG_DIR}/{ITERATION}"):
                            os.makedirs(f"{LOG_DIR}/{ITERATION}")
                        log_file = open(f"{LOG_DIR}/{ITERATION}/{MICRO_BENCHMARK}_{experiment}_cores{core}_cfreq{cfreq}_cint{cint}_{current_time}", 'a')
                        process = subprocess.run([f'{FLINK_HOME}/bin/flink', 'run', '-c', cp, '-Dexecution.runtime-mode=BATCH', 'target/scala-2.12/benchmark-assembly-0.1-SNAPSHOT.jar', str(core), input_file, str(cfreq), str(cint)], text=True, stdout=subprocess.PIPE, check=True)
                        print(process.stdout, file=log_file)
                        os.system('echo 3 > /proc/sys/vm/drop_caches')
                        log_file.flush()
                        log_file.close()

if (__name__ == "__main__"):
    assemble = False

    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        if arg == '-t':
            MICRO_BENCHMARK = sys.argv[i+1]
        elif arg == '-a':
            assemble = True
    
    if ((MICRO_BENCHMARK == "") or (not os.path.exists(f"conf/{MICRO_BENCHMARK}.json"))):
        print("The conf file for the micro-benchmark is not found")
        exit(1)

    f = open(f"conf/{MICRO_BENCHMARK}.json")
    config = json.load(f)

    EXPERIMENTS = config['experiments']
    REPEAT = config['repeat']
    LOG_DIR = config['log_dir']

    FLINK_HOME=config['flink']['home']
   
    subprocess.run(['mkdir', '-p', LOG_DIR], text=True, stdout=subprocess.PIPE, check=True)
    
    if (assemble):
        subprocess.run(['sbt', 'assembly'], text=True, stdout=subprocess.PIPE, check=True)
        print(f"Assemble jar file for deployment completed")

    for i in range(REPEAT):
        ITERATION = i
        run(config['cores'], config['cfreqs'], config['cinterval'])
