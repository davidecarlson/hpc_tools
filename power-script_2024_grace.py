#!/usr/bin/env python

import os
import re
from statistics import mean
import pandas as pd
import argparse
from datetime import datetime, time

# take in two input file arguments, a json file and a text log file
parser = argparse.ArgumentParser(description="Get energy consumption from a log file")
parser.add_argument('--threads', type=int, required=True, help="Number of threads to search for in the log file")
parser.add_argument('--log', type=str, required=True, help="Log file to search through")
parser.add_argument('--runs', type=int, required=True, help="The number of runs to average")
args = parser.parse_args()

log_file = args.log

# define the ip address for fj-grace2
ip_addr = '10.10.1.201'



# read the log file
with open(log_file, 'r') as file:
    lines = file.readlines()

# Regular expressions to find the relevant benchmark, start, and end times
benchmark_pattern = re.compile(r'Benchmark (\d+):.*-(p|T|t|@) (\d+)')
start_time_pattern = re.compile(r'Start Time: (.+)')
end_time_pattern = re.compile(r'End Time: (.+)')

# Variables to hold the found start and end times
found_start_time = None
found_end_time = None

# Parse the log file to find the matching thread count
for i, line in enumerate(lines):
    # Check for a benchmark line and matching thread count
    benchmark_match = benchmark_pattern.match(line)
    if benchmark_match:
        benchmark_number = benchmark_match.group(1)
        thread_count = int(benchmark_match.group(3))
        
        if thread_count == args.threads:
            # Found the correct thread count, now search for start and end times
            for j in range(i + 1, len(lines)):
                start_time_match = start_time_pattern.match(lines[j])
                if start_time_match:
                    found_start_time = start_time_match.group(1).replace('/', '-')
                    #print(found_start_time)
                    # parse found_start_time to datetime object
                    #full_datetime = datetime.strptime(found_start_time, "%m-%d-%y %H:%M:%S")
                    #print(full_datetime)
                
                end_time_match = end_time_pattern.match(lines[j])
                if end_time_match:
                    found_end_time = end_time_match.group(1).replace('/', '-')
                    # parse found_end_time to datetime object
                    #found_end_time = datetime.strptime(found_end_time, "%m-%d-%y %H:%M:%S")

                    break
            break


# Display the result
if found_start_time:
    time_start = datetime.strptime(found_start_time, "%m-%d-%y %H:%M:%S")
    time_end = datetime.strptime(found_end_time, "%m-%d-%y %H:%M:%S")
    date =  time_start.strftime('%Y%m%d')
    day = time_start.strftime('%d')
    month = time_start.strftime('%m')
    year = time_start.strftime('%Y')
    print(f"Benchmark with -{benchmark_match.group(2)} {args.threads}")
    print(f"Start Time: {time_start}")
    print(f"End Time: {time_end}")
else:
    print(f"No benchmark found with -p {args.threads}")

#start_time_only = time.fromisoformat(time_start)
start_time_only = time_start.time()
end_time_only = time_end.time()

file_to_open = f'/lustre/admin/power_monitoring/power/{year}/{year}{month}/{month}{day}/power_orginfo_{ip_addr}_{date}.csv'

columns =['time','power']
df=pd.read_csv(file_to_open,sep=',',header=None,names=columns)
df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time


#Replace any error messages with NaN so the averaging can work
df['power'] = pd.to_numeric(df['power'], errors='coerce')

# job duration
timedelta = pd.to_datetime(end_time_only,format='%H:%M:%S') - pd.to_datetime(start_time_only,format='%H:%M:%S')

duration =  timedelta.seconds / 60.0 / 60.0
print(f'Job duration: {duration:.3f} hours')

#get power data usage for the time when the job was running, excluding the first and last 2 minutes unless the job was shorter than 5 minutes

if duration < 0.0833:
    job_df = df.loc[(df['time'] > start_time_only) & (df['time'] < end_time_only)]
else:
    job_df = df.loc[(df['time'] > start_time_only) & (df['time'] < end_time_only)].iloc[2:-1]

print(job_df)
# get the average power consumption over the time period the job was running
power_sum = job_df['power'].mean()
print(f'Average power usage while job running: {power_sum:.3f} W')

energy = power_sum * duration
print(f'Energy consumption across {args.runs} runs: {energy:.3f} Wh')
mean_energy = energy / args.runs
print (f'Average energy used per run: {mean_energy:.3f} Wh')



