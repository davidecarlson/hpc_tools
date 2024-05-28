#!/gpfs/software/Anaconda/bin/python
# -*- coding: utf-8 -*-

import subprocess
import re
import pandas as pd
from tabulate import tabulate
import argparse

parser = argparse.ArgumentParser(description="Calculate CPU and Memory usage for each node running a Slurm job")
parser.add_argument("-u", "--user", help="Only report usage for this user", required=False)
parser.add_argument("-l", "--low", help="Only report nodes with %% CPU usage lower than this value", required=False)
parser.add_argument("-e", "--high", help="Only report nodes with %% CPU usage higher than this value", required=False)
parser.add_argument("-n", "--node", help="Only report usage on this node", required=False)
parser.add_argument("-j", "--job", help="Only report usage for this job ID", required=False)
args = parser.parse_args()


def node_stats():
    # find the status of all allocated nodes except for those running A100 gpu jobs and shared jobs
    sinfo_cmd="/cm/shared/apps/slurm/current/bin/sinfo -a --Node -o '%.10N %8O %c %.10e %.10m  %.5a %.6t %12E %G'|uniq|grep alloc|grep -Ev 'a100|shared|rn' | awk '{print $1,$2,$3,$4,$5}'"
    sinfo = subprocess.getoutput(sinfo_cmd)
    sinfo = sinfo.split("\n")
    sinfo = [x.split() for x in sinfo]
    sinfo_stats = pd.DataFrame(sinfo, columns=["Node", "CPU load", "CPUs available", "Memory available (MB)", "Total Memory (MB)"])
    sinfo_stats["% CPUs used"] = sinfo_stats["CPU load"].astype(float) / sinfo_stats["CPUs available"].astype(float) * 100
    sinfo_stats["% Memory used"] = (sinfo_stats["Total Memory (MB)"].astype(float) - sinfo_stats["Memory available (MB)"].astype(float)) /sinfo_stats["Total Memory (MB)"].astype(float)  * 100
    sinfo_stats = sinfo_stats[["Node", "CPU load", "% CPUs used", "% Memory used"]]
    return(sinfo_stats)

def expand_nodelist(nodelist):
    """
    Expand the nodelist to handle ranges and comma-separated lists.
    Example: "dg[035-036,042]" -> ["dg035", "dg036", "dg042"]
    """
    pattern = re.compile(r'(\D+)\[(.+)\]')
    match = pattern.match(nodelist)
    
    if not match:
        return [nodelist]
    
    prefix, ranges = match.groups()
    nodes = []
    
    for part in ranges.split(','):
        if '-' in part:
            start, end = part.split('-')
            nodes.extend([f"{prefix}{str(i).zfill(len(start))}" for i in range(int(start), int(end) + 1)])
        else:
            nodes.append(f"{prefix}{part}")
    return(nodes)

def get_job_ids_by_node(node_info):
    # Join the list of nodes into a comma-separated string
    nodelist= ",".join([node for node in node_info["Node"]])
    #print(nodelist)
    
    # Run the squeue command with the specified nodes and capture the output
    result = subprocess.run(['/cm/shared/apps/slurm/current/bin/squeue', '-a', '-w', nodelist, '-o', '%.18i %.6D %R'], stdout=subprocess.PIPE)
    
    # Decode the output to string
    output = result.stdout.decode('utf-8')
    
    # Split the output into lines
    lines = output.split('\n')
    
    # Create a list to store job IDs and corresponding nodes
    job_node_pairs = []
    
    # Process each line of the squeue output
    for line in lines[1:]:
        if line:
            parts = line.split()
            job_id = parts[0]
            nodelist = parts[-1]
            
            # Expand the nodelist
            expanded_nodes = expand_nodelist(nodelist)
            
            # Add job ID and each node to the list of pairs
            for node in node_info["Node"]:
                if node in expanded_nodes:
                    job_node_pairs.append((job_id, node))
    df = pd.DataFrame(job_node_pairs, columns=['Job ID', 'Node'])
    combined_df = node_info.merge(df, on='Node', how='outer')
    return(combined_df)

def slurm_jobs(sinfo_stats):
    nodelist= ",".join([node for node in sinfo_stats["Node"]])
    squeue_cmd="/cm/shared/apps/slurm/current/bin/squeue -w {node} -o '%10i %22P %16j %12u %.10M %D %N' -ahw " + nodelist
    jobs = subprocess.getoutput(squeue_cmd)
    jobs = jobs.split("\n")
    jobs = [x.split() for x in jobs]
    jobs = pd.DataFrame(jobs, columns=["Job ID", "Partition", "Job Name", "User","Time", "# nodes", "Job nodelist"])
    return(jobs)

if __name__ == "__main__":
    node_info = node_stats()
    node_jobid_info = get_job_ids_by_node(node_info)
    jobs = slurm_jobs(node_info)
    if args.user:
        jobs = jobs[jobs["User"] == args.user]
    if args.low:
        node_jobid_info = node_jobid_info[node_jobid_info["% CPUs used"] < float(args.low)]
    if args.high:
        node_jobid_info = node_jobid_info[node_jobid_info["% CPUs used"] > float(args.high)]
    if args.node:
        node_jobid_info = node_jobid_info[node_jobid_info["Node"] == args.node]
    if args.job:
        node_jobid_info = node_jobid_info[node_jobid_info["Job ID"] == args.job]
    final_data = pd.merge(node_jobid_info, jobs, on="Job ID")
    #final_data = node_jobid_info.merge(jobs, on='Job ID', how='outer')
    print(tabulate(final_data, headers="keys", tablefmt="psql", showindex=False))

