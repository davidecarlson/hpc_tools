import pandas as pd
import sys
from datetime import datetime
import time
import subprocess

jobid = sys.argv[1]

job = subprocess.run(['sacct', '-j', jobid,'--format=Start,End,AllocNodes,NodeList%30', '--noheader'],stdout=subprocess.PIPE, universal_newlines = True)

results = job.stdout.split(' ')

# remove empty elements in results list
cleaned = [i for i in results if i]

time_start = cleaned[0].split('T')[1]
time_end = cleaned[1].split('T')[1]

number_nodes = cleaned[2]
nodes = cleaned[3]

# getting nodelist
node_list = [0] * int(number_nodes)
node_x = nodes[3:len(nodes)-1]
if int(number_nodes) == 1:
    node_list[0] = int(nodes[3:6])
else:
        for x in node_x.split(','):
            if '-' in x:
                lnum, rnum = x.split('-')
                lnum, rnum = int(lnum), int(rnum)
                node_list.extend(range(lnum, rnum + 1))
            else:
                lnum = int(x)
                node_list.append(lnum)
           

date = cleaned[0].split('T')[0].replace('-','')
year = date[0:4]
month = date[4:6]
day = date[6:8]

sum = float(0)
count = int(0)
cpu = nodes[0:2]
for i in range(0, len(node_list)):
    j = int(node_list[i])
    if j == 0:
        continue
    else:
        count += 1
        if cpu == 'xm':
           ip_addr = f'10.10.9.{str(j)}'
        else:
            if j < 10:
               index = '0' + str(j)
            else:
               index =  str(j)
        if cpu == 'dg':
            ip_addr = f'10.10.9.2{index}'
        elif cpu == 'dn':
            ip_addr = f'10.10.9.1{index}'
        if (j != 0):
            file_to_open = f'/gpfs/power_monitoring/power/{year}/{year}{month}/{month}{day}/power_orginfo_{ip_addr}_{date}.csv'
            columns =['time','power']
            df=pd.read_csv(file_to_open,sep=',',header=None,names=columns)
            times=pd.to_datetime(df['time'],format='%H:%M:%S').dt.time
            #Replace any error messages with NaN so the averaging can work
            df['power'] = pd.to_numeric(df['power'], errors='coerce')
            #excluded first 2 and last data point
            #power_sum = df.loc[(df['time'] > time_start) & (df['time'] < time_end), 'power'].mean()
            job_df = df.loc[(df['time'] > time_start) & (df['time'] < time_end)].iloc[2:-1] 
            power_sum = job_df['power'].mean()
            sum += power_sum
# job duration
timedelta = pd.to_datetime(time_end,format='%H:%M:%S') - pd.to_datetime(time_start,format='%H:%M:%S')
duration =  timedelta.seconds / 60.0 / 60.0

energy = sum * duration
print (energy, 'Wh')
