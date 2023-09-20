#!/usr/bin/env python

# calculate the CPU usage for each running Slurm job

import os
import subprocess
import socket
import pandas as pd
import re
from datetime import date
from tabulate import tabulate



def findval(str):
    # take a range of values (e.g., 1-10) and return a list of all the values in that range
    # from https://stackoverflow.com/questions/20662764/expand-a-string-describing-a-set-of-numbers-noted-as-a-list-of-numbers-and-or-ra
    val = []
    for x in str.split(','):
        #print(x)
        if '-' in x:
            lnum, rnum = x.split('-')
            lnum, rnum = int(lnum), int(rnum)
            val.extend(range(lnum, rnum + 1))
        else:
            lnum = int(x)
            val.append(lnum)
    
    return(val)

def split_nodes(lst):

    if 'sn[' in lst:
        stripped = lst.strip('sn[').strip(']')
        numlist = findval(stripped)
        nodelist = ['sn' + str(num).zfill(3) for num in numlist]      
    elif 'cn[' in lst:
        stripped = lst.strip('cn[').strip(']')
        numlist = findval(stripped)
        nodelist = ['cn' + str(num).zfill(3) for num in numlist]
    elif 'dn[' in lst:
        stripped = lst.strip('dn[').strip(']')
        numlist = findval(stripped)
        nodelist = ['dn' + str(num).zfill(3) for num in numlist]
    elif 'dg[' in lst:
        stripped = lst.strip('dg[').strip(']')
        numlist = findval(stripped)
        nodelist = ['dg' + str(num).zfill(3) for num in numlist]
    else:
        nodelist=lst
    #print(nodelist)
    return(nodelist)


def slurm_jobs():

    jobs = subprocess.run(['squeue', '-o',  "%.25u %.12A %.25N", '-t', 'R', '--noheader'],
        stdout=subprocess.PIPE, universal_newlines = True)
    #jobs = subprocess.run(['squeue', '-u', 'asingal', '-o',  "%.25u %.12A %.25N", '-t', 'R', '--noheader'],
    #    stdout=subprocess.PIPE, universal_newlines = True)

    results = jobs.stdout.splitlines()
   
    # convert multiple spaces to a single space

    new_results = [re.sub(' +', ' ', result).strip(' ').split(' ') for result in results]
    reformatted = [[result[0], result[1], split_nodes(result[2])] for result in new_results]

    df = pd.DataFrame(reformatted, columns = ['User', 'Job ID', 'Nodelist'])

    # convert df to long format so that there is one row per node (multiple rows for multinode jobs)
    df2 = df.explode('Nodelist')
    # remove rows for gpu nodes and rizzo nodes from the df
    cpu_jobs = df2[df2["Nodelist"].str.contains('nv|rn|a100')==False]
    return(cpu_jobs)

def coreusage(node,user):
    print(f'Now looking up core usage on node {node}...')
    # only using first six characters of username because ps aux cuts the rest off
    command_cpu = f'ssh {node} "ps aux | grep {user[0:7]} | grep -v grep"'
    ssh_cmd = subprocess.run(command_cpu, capture_output=True, universal_newlines = True, shell =True)
    results_cpu = ssh_cmd.stdout.splitlines()
    cpus = [float(result.split()[2]) for result in results_cpu]
    node_usage = sum(cpus)
    return(node_usage)

def memusage(node,user):
    print(f'Now looking up memory usage on {node}...')
    command_mem = f'ssh {node} "ps -U {user} --no-headers -o rss"'
    ssh_cmd = subprocess.run(command_mem, capture_output=True, universal_newlines = True, shell =True)
    results_mem = ssh_cmd.stdout.splitlines()
    mem = [float(result) for result in results_mem]
    # get total rss memory usage in Gb
    total_mem = sum(mem) / 01e06
    return(total_mem)

def num_cores(node):
    if 'sn' in node:
        cores = 28
    elif 'cn' in node:
        cores = 24
    elif 'dn' in node:
        cores = 40
    elif 'dg' in node:
        cores = 96
    elif 'a100' in node:
        cores = 64
    else:
        cores = 'N/A'
    return(cores)
    
def get_stats(job_df):
    # apply the nodeuseage function to each row of the jobs df
    print(job_df)
    job_df['Cores Used'] = job_df.apply(lambda x: coreusage(x['Nodelist'], x['User']), axis = 1)
    job_df['Memory Used (GB)'] = job_df.apply(lambda x: memusage(x['Nodelist'], x['User']), axis = 1)
    # apply the num_cores function to each row of the jobs ds
    job_df['Cores available'] = job_df.apply(lambda x: num_cores(x['Nodelist']), axis = 1)

    # use groupby to aggregate stats across multinode jobs and combine the resulting dataframes back together
    cores_per_job = job_df.groupby(['Job ID'],as_index=False)['Cores Used'].sum()
    memory_per_job = job_df.groupby(['Job ID'],as_index=False)['Memory Used (GB)'].sum()
    print(memory_per_job)
    cores_per_job['Cores Used'] = cores_per_job['Cores Used'] / 100
    cores_avail_per_job = job_df.groupby(['Job ID'],as_index=False)['Cores available'].sum()
    nodes_per_job = job_df.groupby(['Job ID'],as_index=False)['Nodelist'].count()
    user_per_job = job_df.groupby(['Job ID', 'User'],as_index=False).size()
    

    combined_job_stats = cores_per_job.merge(nodes_per_job, on = 'Job ID', how = 'inner') \
    .merge(user_per_job, on = 'Job ID', how = 'inner') \
    .merge(cores_avail_per_job,on = 'Job ID', how = 'inner') \
    .merge(memory_per_job,on = 'Job ID', how = 'inner')

    # clean up the dataframe
    renamed = combined_job_stats.rename({'Nodelist': 'Node Count'}, axis=1)
    print(renamed)
    renamed['Job efficiency (% total cores)'] = renamed['Cores Used'] / renamed['Cores available'] * 100
    renamed.drop('size', axis = 1, inplace = True)

    return(renamed)

def get_date():
    today = date.today()
    pretty_date = today.strftime("%b-%d-%Y")
    return(pretty_date)


def write_results(results_df):
    # write output to a date-stamped TSV file
    output = '~/slurm_job_stats'
    today = get_date()
    # get hostname so we can write out separate results for SW2 and SW3
    host = socket.gethostname()
    if 'dg' in host or  'milan' in host or 'xeonmax' in host:
        sw_version = 'seawulf3'
    elif 'login' in host or 'cn' in host:
        sw_version = 'seawulf2'
    results_df.to_csv(f'{output}/{today}_{sw_version}_job_stats.txt',sep='\t',index=False, float_format='%.3f')


if __name__=='__main__':
    print("Looking up all running Slurm jobs...\n")
    jobs = slurm_jobs()
    #job_subset = jobs.head(10)
    print("Calculating stats for each job...\n")
    final_results = get_stats(jobs)
    print("writing final results to tab-separated output file\n")
    print(tabulate(final_results, headers='keys', tablefmt='psql', showindex=False))
    write_results(final_results)

