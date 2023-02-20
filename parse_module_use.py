#!/usr/bin/env python

# read in module use data from sys logs, calculate and return user requested information

import argparse
import sys
import os
import subprocess
import pandas as pd
import ast
import datefinder
import datetime
from datetime import date
from itertools import chain
from tabulate import tabulate

parser = argparse.ArgumentParser(description="Generate stats regarding module use from data collected from HPC users")

parser.add_argument('--log', required=True, help='system log file with module usage', action='store')
parser.add_argument('--module', required=False, help='Name of specific module to get information about', action='store')
parser.add_argument('--general', required=False,action=argparse.BooleanOptionalAction,help='Get general information and stats about module use')
parser.add_argument('--full', required=False,action=argparse.BooleanOptionalAction, help='Print full table of loaded modules')
parser.add_argument('--recent', required=False, action='store', help='Print the N most recently loaded modules', type=int, default=10)
parser.add_argument('--top', required=False, action='store', help='Print top N most frequently loaded modules', type=int, default=10)
parser.add_argument('--user', required=False, action='store', help='Get general useage info for specific user', type=str)
parser.add_argument('--start', required=False, action='store', help='Get modules loaded after specific date (format: YYYY-DD-MM)',
    type=datetime.date.fromisoformat)
parser.add_argument('--end', required=False, action='store', help='Get modules loaded up to (inclusive) specific date (format: YYYY-DD-MM). Default is today\'s date',
    type=datetime.date.fromisoformat, default = date.today())
parser.add_argument('--prefix_all', required=False,action=argparse.BooleanOptionalAction,help='Get information on all modules that match the module prefix')

args=parser.parse_args()

file = args.log
mod_name = args.module
prefix = args.prefix_all
topN = args.top
recentN = args.recent
user = args.user
start = args.start
end = args.end


# read in the module usage log file
def read_file(file):
    with open(file) as f:
        data = f.readlines()
        # only keep entries where the "load" command was issued
        # Date format changed on Feb 3 2023, so need to treat old and new formats separately
        loaded_old = [entry for entry in data if "load " in entry if "unload" not in entry if entry.startswith(("Feb", "Jan"))]
        loaded_new = [entry for entry in data if "load " in entry if "unload" not in entry if entry.startswith("20")]
        
    return(loaded_old, loaded_new)

def get_year():
    today = datetime.date.today()
    year = str(today.year)
    return(year)

# check for valid module file name
def check_mod(mod_name):
    # load the python implementation of environment modules
    sys.path.insert(0, '/cm/local/apps/environment-modules/current/init')
    from python import module


    found = module('whatis', mod_name)
    if found != True:
        print()
        print("Module not found. Please check name and try again.")
        print()
        exit()

# parse the data into a more useful format

def reformat_data_old(data):

    dates = [' '.join(entry.split()[0:3]) for entry in data]
    # use datefinder to get properly formated date and time
    matches = [datefinder.find_dates(date) for date in dates]

    corr_dates = [days.strftime("%Y-%m-%d %H:%M:%S") for days in chain.from_iterable(matches)]

    nodes = [entry.split()[3] for entry in data]
    users = [entry.split(':')[4].split(',')[0].strip().replace('"', '') for entry in data]

    loaded_modules = [entry.split('load ')[-1].replace('}', '').replace('"', '').replace('{', '').strip().replace(' ', ',') for entry in data]

    module_df = pd.DataFrame(
        {   "dates": corr_dates,
            "nodes": nodes,
            "users": users,
            "modules": loaded_modules
        }
    )

    return(module_df)

def reformat_data_new(data):

    dates = [entry.split()[0] for entry in data]
    
    # use datefinder to get properly formated date and time
    matches = [datefinder.find_dates(date) for date in dates]

    corr_dates = [days.strftime("%Y-%m-%d %H:%M:%S") for days in chain.from_iterable(matches)]

    nodes = [entry.split()[1] for entry in data]

 
    users = [entry.split(':')[5].split(',')[0].strip().replace('"', '') for entry in data]

    loaded_modules = [entry.split('load ')[-1].replace('}', '').replace('"', '').replace('{', '').strip().replace(' ', ',') for entry in data]

    module_df = pd.DataFrame(
        {   "dates": corr_dates,
            "nodes": nodes,
            "users": users,
            "modules": loaded_modules
        }
    )

    return(module_df)

def combine_dfs(old, new):
    combined = pd.concat([old,new])

    # because some entries in the dataframe have multiple modules that were loaded in the same command, need to split each into their own row
    # turn each cell with multiple modules into a list
    combined['modules']=combined['modules'].str.split(',')    
    
    # explode the module column to make a separate row for each entry in the list
    combined_2 = combined.explode('modules')
    
    # remove rows where the slurm and shared modules are loaded because we don't care about these
    combined_final = combined_2[(combined_2['modules'] != 'shared') & (~combined_2['modules'].str.contains("slurm"))]
    return(combined_final)

def count_usage(df, module):
    # count how often a module has been loaded and how often specific users loaded it

    # check if --prefix-all flags has been given

    if prefix != None:
        subset = df[df['modules'].str.contains(module.split('/')[0])]

    else:
        subset = df[df['modules']==module]
    
    user_count = subset.groupby('users',as_index=False)['modules'].count().rename(columns={'modules': '# of times loaded'})
    counts = len(subset.index)
    
    if counts > 0:
        #counts = df['modules'].value_counts()[module]
        print()
        print('###########################################################################')
        print(f'{module} load count: \n \t {counts}')
        print('###########################################################################')
        print()
        print(f"The following table shows which users have loaded the {module} module and how many times they have loaded it.")
        print()
        print(tabulate(user_count, headers='keys', tablefmt='psql'))
        print()
        print('##########################################################')
        print(f'Most recent {module} load date:       \n \t{subset["dates"].max()}')
        print('##########################################################')

    
    elif counts == 0:
        print()
        print('##########################################################')
        print(f'The {module} module has not been loaded loaded since tracking began.')
        print('##########################################################')
        print()

def genstat(df, top=topN):
    # get basic summary stats regarding module usage
    total_loaded = df['modules'].nunique()

    total_users = df['users'].nunique()
    
    mod_count = df.groupby(['modules'],as_index=False).count().sort_values(['dates'], ascending=False).rename(columns={'users': '# of times loaded'}).drop(['dates', 'nodes'], axis=1)
    
    print()
    print('##########################################################')
    print('Here are some general module usage stats:')

    print()
    print(f'Total number of unique modules loaded:    \n \t{total_loaded}')
    print
    print()
    print
    print(f'Total number of users:    \n \t{total_users}')
    print('##########################################################')
    print()
    print(f'The following table shows the {top} most frequently loaded modules:')
    print()
    print(tabulate(mod_count.head(top), headers='keys', tablefmt='psql', showindex="never"))
    
def full(df):
    # print full table of module use results
    print()
    print('The following table lists all modules loaded since tracking began.')
    print(tabulate(df, headers='keys', tablefmt='psql', showindex="never"))

def recent(df,recent=recentN):
    date_df = df.sort_values(by='dates', ascending=False)
    print()
    print(f'The following table shows the {recent} most recently loaded modules:')
    print()
    print(tabulate(date_df.head(recent), headers='keys', tablefmt='psql', showindex="never"))

def byuser(df, user):
    if user != None:
        user_subset = df[df['users']==user]
        total_user_loaded = user_subset['modules'].nunique()

        if total_user_loaded > 0:

            print()
            print('##########################################################')
            print(f'{user} has loaded {total_user_loaded} different modules')
            print()
            print('##########################################################')
            print()
            print(f'The following table shows the modules most recently loaded by {user}:')
            print(tabulate(user_subset, headers='keys', tablefmt='psql', showindex="never"))

        else:
            print()
            print('#######################################################################')
            print(f'No modules ever loaded by user {user}. Please double check NetID.')
            print()
            print('#######################################################################')

def bydate(df, start=start, end=end):

    df['dates'] = pd.to_datetime(df['dates']).dt.date

    date_subset = df[(df['dates'] > start) & (df['dates'] <= end)]

    print(f'The following table shows the modules loaded between {start} and {end}')
    print(tabulate(date_subset, headers='keys', tablefmt='psql', showindex="never"))

def bydate_and_user(df, start=start, end=end, user=user):

    df['dates'] = pd.to_datetime(df['dates']).dt.date
    date_user_subset = df[(df['dates'] >= start) & (df['dates'] <= end) & (df['users'] == user)]

    if len(date_user_subset.index) > 0:

        print(f'The following table shows the modules loaded between {start} and {end} by {user}')
        print(tabulate(date_user_subset, headers='keys', tablefmt='psql', showindex="never"))
    else:
        print()
        print('#######################################################################')
        print(f'No modules loaded by user {user}. Please double check NetID and date range.')
        print()
        print('#######################################################################')

    
if __name__=='__main__':
    
    olddata, newdata = read_file(file)
    old_data_df = reformat_data_old(olddata)
    new_data_df = reformat_data_new(newdata)
    data_df = combine_dfs(old_data_df,new_data_df)
    if mod_name != None:
        check_mod(mod_name)
        count_usage(data_df, mod_name)
    if args.general != None:
        genstat(data_df, top=topN)
        recent(data_df, recent=recentN)
    if args.full != None:
        full(data_df)
    if user != None and start == None:
        byuser(data_df, user)
    elif start != None and user ==None:
        bydate(data_df, start, end)
    elif start !=  None and user != None:
        bydate_and_user(data_df, start, end, user)
