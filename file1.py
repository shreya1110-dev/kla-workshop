#!/usr/bin/env python

from typing import MutableMapping
import yaml
from datetime import datetime
import time
import pandas as pd
from collections.abc import MutableMapping
import multiprocessing
import threading

def read_yaml():
    with open("Milestone1/Milestone1A.yaml", "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            return 0

def timeFunction(x):
    time.sleep(x)

def dataLoad(inp,op):
    op = pd.read_csv(inp)
    return len(op)

def read_dict(d: MutableMapping, parent_key: str = '', sep: str ='.'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(read_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def sequential(count, workflow_list, time1, f):
    t = time1
    for i in range(count, len(workflow_list)):
        if(workflow_list[i][1] == "Concurrent"):
            break
        parser = workflow_list[i][0].split('.')
        if(parser[1] == "Activities" and parser[len(parser)-1]=="Type" and workflow_list[i][1] == "Task"):
            timestamp = str(t) + ","
            timestamp = timestamp + parser[0] + " " + parser[len(parser)-2] + " " + "Entry\n"
            print("hello")
            f.write(timestamp)
        if(parser[1] == "Activities" and parser[len(parser)-1]=="Function"):
            if(workflow_list[i][1] == "TimeFunction"):
                timestamp = str(t) + ","
                timestamp = timestamp + "Executing TimeFunction "
                arg1 = workflow_list[i+1][1]
                arg2 = workflow_list[i+2][1]
                timestamp = timestamp + "(" + arg1 + "," + arg2 + ")\n"
                f.write(timestamp)
        if(parser[1] == "Activities" and parser[len(parser)-1]=="ExecutionTime"):
            print("hi")
            time.sleep(int(workflow_list[i][1].strip()))
            t = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            timestamp = t + ","
            print(parser[len(parser)-2])
            timestamp = timestamp + parser[0] + " " + parser[len(parser)-3] + " " + "Exit\n"
            f.write(timestamp)

def concurrent(count, workflow_list, time1, f):
    t = time1
    for i in range(count, len(workflow_list)):
        if(workflow_list[i][1] == "Sequential"):
            break
        parser = workflow_list[i][0].split('.')
        if(parser[1] == "Activities" and parser[len(parser)-1]=="Type" and workflow_list[i][1] == "Task"):
            timestamp = str(t) + ","
            timestamp = timestamp + parser[0] + " " + parser[len(parser)-2] + " " + "Entry\n"
            f.write(timestamp)
        if(parser[1] == "Activities" and parser[len(parser)-1]=="Function"):
            if(workflow_list[i][1] == "TimeFunction"):
                timestamp = str(t) + ","
                timestamp = timestamp + "Executing TimeFunction "
                arg1 = workflow_list[i+1][1]
                arg2 = workflow_list[i+2][1]
                timestamp = timestamp + "(" + arg1 + "," + arg2 + ")\n"
                f.write(timestamp)
        if(parser[1] == "Activities" and parser[len(parser)-1]=="ExecutionTime"):
            t = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            timestamp = t + ","
            timestamp = timestamp + parser[0] + " " + parser[len(parser)-4] + " " + "Exit\n"
            f.write(timestamp)

def print_log():
    data = read_yaml()
    workflow = read_dict(data)
    timestamp = ''
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    timestamp += timestamp + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    timestamp += ','
    key = data.keys()
    for k in key:
        timestamp += k
        timestamp += " Entry\n"
    f = open("file1.txt", "a")
    f.write(timestamp)
    count = 0
    workflow_list = list(workflow.items())
    for i in range(0, len(workflow_list)):
        print(workflow_list[i])
    for i in range(0, len(workflow_list)):
        parser = workflow_list[i][0].split('.')
        if(workflow_list[i][1] == "Sequential" and parser[len(parser)-1]=="Execution"):
            sequential(count, workflow_list, time, f)
        else:
            concurrent(count, workflow_list, time, f)
        count = count+1
    timestamp = ''  
    timestamp += timestamp + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    timestamp += ','
    key = data.keys()
    for k in key:
        timestamp += k
        timestamp += " Entry\n"
    f.write(timestamp)      
    f.close()
print_log()