#!/usr/bin/python3
import re
import sys
import argparse
import itertools

parser = argparse.ArgumentParser()
parser.add_argument('--task', type=str, choices=['filter', 'ensure_no_overlap', 'select'])
parser.add_argument('--include', type=str)
parser.add_argument('--exclude', type=str, default='')
parser.add_argument('--count', type=int, default=0, help='echo count of nodes in result or include')
parser.add_argument('--select', type=int, default=-1, help='used only in select mode, output a subset of nodes from the include list. -1 means select all')
args = parser.parse_args()

nodelist_arg = args.include
exclude_arg = args.exclude 

#compute-dy-p4d24xlarge-
#compute-st-p4d24xlarge-
exclude = {'dy': [], 'st': []}
include = {'dy':[], 'st': []}

def ranges(i):
    for a, b in itertools.groupby(enumerate(i), lambda pair: pair[1] - pair[0]):
        b = list(b)
        yield b[0][1], b[-1][1]

def build_map(dic, arg):
    for r in arg.split('compute-'):
        r = r.strip(',')
        if not r:
            continue
        #todo: move below to regex
        dyn_or_st = r.split('-',2)[0]
        numbers = r.split('-', 2)[-1]
        numbers = numbers.strip('[]')
        for subr in numbers.split(','):
            if '-' in subr:
                for n in range(int(subr.split('-')[0]), int(subr.split('-')[1]) + 1):
                    dic[dyn_or_st].append(n)
            else:
                dic[dyn_or_st].append(int(subr))

def print_final_selected(selected, identifier):
    rval = []
    for typ in selected:
        selected[typ].sort()
        if len(selected[typ]):
            rval.append(f'compute-{typ}-{identifier}-')
            if len(selected[typ]) == 1:
                rval[-1] += f'{selected[typ][0]}'
            else:
                rval[-1] += '['
                for s,e in ranges(selected[typ]):
                    if s==e:
                        rval[-1] += f'{s},'
                    else:
                        rval[-1] += f'{s}-{e},'
                rval[-1] = rval[-1][0:-1] + ']'
    print(','.join(rval), end=' ')


if args.task in ['filter', 'ensure_no_overlap', 'select']:
    build_map(exclude, exclude_arg)
    build_map(include, nodelist_arg)
    
    if 'worker' in exclude_arg or 'worker' in nodelist_arg:
        identifier = 'worker'
    else:
        identifier = 'p4d24xlarge'

    final_include = {'dy': [], 'st': []}
    for typ in include:
        for n in include[typ]:
            if args.task in ['filter', 'select'] and n not in exclude[typ] and n not in final_include[typ]:
                final_include[typ].append(n)
            elif args.task == 'ensure_no_overlap' and n in exclude[typ]:
                print('Error: Given include and exclude overlap, returning exit code 1')
                sys.exit(1)

    if args.task == 'select':
        if args.select == -1:
            print_final_selected(final_include, identifier)
        else:
            total = len(final_include['dy']) + len(final_include['st'])
            if (args.select > total):
                print(f'Error: Asking for {args.select} nodes while you only got {total}')
                sys.exit(1)
            len_st = len(final_include['st'])
            if args.select <= len_st:
                final_include['st'] = final_include['st'][0:args.select]
                final_include['dy'] = []
            else:
                args.select -= len_st
                final_include['dy'] = final_include['dy'][0:args.select]
            print_final_selected(final_include, identifier)

    if args.task == 'filter':
        print_final_selected(final_include, identifier)
        if args.count:
            print(len(final_include['dy'])+len(final_include['st']), end=' ')
