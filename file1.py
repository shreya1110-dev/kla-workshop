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
    with open("Milestone1/Milestone1B.yaml", "r") as stream:
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
    flag = 0
    for i in range(count, len(workflow_list)):
        if(workflow_list[i][1] == "Concurrent"):
            break
        parser = workflow_list[i][0].split('.')
        if(parser[1] == "Activities" and parser[len(parser)-1]=="Type" and workflow_list[i][1] == "Task"):
            timestamp = str(t) + ";"
            if(len(parser)>5):
                timestamp = timestamp + parser[0] + "." + parser[len(parser)-4] + "." + parser[len(parser)-2] + " " + "Entry\n"
            else:
                timestamp = timestamp + parser[0] + "." + parser[len(parser)-2] + " " + "Entry\n"
            f.write(timestamp)
        if(parser[1] == "Activities" and parser[len(parser)-1]=="Function"):
            if(workflow_list[i][1] == "TimeFunction"):
                timestamp = str(t) + ";"
                if(len(parser)>5):
                    timestamp = timestamp + parser[0] + "." + parser[len(parser)-4] + "." + parser[len(parser)-2]
                else:
                    timestamp = timestamp + parser[0] + "." + parser[len(parser)-2]
                timestamp = timestamp + " Executing TimeFunction "
                arg1 = workflow_list[i+1][1]
                arg2 = workflow_list[i+2][1]
                timestamp = timestamp + "(" + arg1 + ", " + arg2 + ")\n"
                f.write(timestamp)
        if(parser[1] == "Activities" and parser[len(parser)-1]=="ExecutionTime"):
            print("hi")
            time.sleep(int(workflow_list[i][1].strip()))
            t = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            timestamp = t + ";"
            print(parser[len(parser)-2])
            if(len(parser)>5):
                timestamp = timestamp + parser[0] + "." + parser[len(parser)-5] + "." + parser[len(parser)-3] + " " + "Exit\n"
            else:
                timestamp = timestamp + parser[0] + "." + parser[len(parser)-3] + " " + "Exit\n"
            f.write(timestamp)
        if(parser[1] == "Activities" and parser[len(parser)-1]=="Type" and workflow_list[i][1]=="Flow"):
            t = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            timestamp = t + ";"
            timestamp = timestamp + parser[0] + "." + parser[len(parser)-2] + " Entry\n"
            f.write(timestamp)
            flag = 1

def print_task(taskname, exectime, time1, parser, f):
    timestamp = time1 + ";"
    if(len(parser)>5):
        timestamp = timestamp + parser[0] + "." + parser[len(parser)-5] + "." + parser[len(parser)-3] + " " + "Entry\n"
    else:
        timestamp = timestamp + parser[0] + "." + parser[len(parser)-3] + " " + "Entry\n"
    print(timestamp)
    f.write(timestamp)
    time.sleep(int(exectime.strip()))
    timestamp = time1 + ";"
    if(len(parser)>5):
        timestamp = timestamp + parser[0] + "." + parser[len(parser)-5] + "." + parser[len(parser)-3]
    else:
        timestamp = timestamp + parser[0] + "." + parser[len(parser)-3]
        timestamp = timestamp + " Executing TimeFunction "
        timestamp = timestamp + "(" + taskname + ", " + exectime + ")\n"
    print(timestamp)
    f.write(timestamp)
    print(parser)
    timestamp = time1 + ";"
    if(len(parser)>5):
            timestamp = timestamp + parser[0] + "." + parser[len(parser)-5] + "." + parser[len(parser)-3] + " " + "Exit\n"
    else:
            timestamp = timestamp + parser[0] + "." + parser[len(parser)-3] + " " + "Exit\n"
    print(timestamp)
    f.write(timestamp)

    

def concurrent(count, workflow_list, time1, f):
    cnt = 0
    threads = []
    for i in range(count, len(workflow_list)):
        parser = workflow_list[i][0].split('.')
        if(workflow_list[i][1] == "Sequential"):
            break
        if(parser[1] == "Activities" and parser[len(parser)-1]=="FunctionInput"):
            cnt = cnt+1
            threads.append(threading.Thread(target=print_task,args=(workflow_list[i][1], workflow_list[i+1][1],time1,parser,f)))
    for i in range(0,cnt):
        threads[i].start()
        threads[i].join()
        
  

def print_log():
    data = read_yaml()
    workflow = read_dict(data)
    timestamp = ''
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    timestamp += timestamp + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    timestamp += ';'
    key = data.keys()
    for k in key:
        timestamp += k
        timestamp += " Entry\n"
    f = open("file2.txt", "a")
    f.write(timestamp)
    count = 0
    workflow_list = list(workflow.items())
    for i in range(0, len(workflow_list)):
        print(workflow_list[i])
    for i in range(0, len(workflow_list)):
        parser = workflow_list[i][0].split('.')
        if(workflow_list[i][1] == "Sequential" and parser[len(parser)-1]=="Execution"):
            sequential(count, workflow_list, time, f)
        elif(workflow_list[i][1] == "Concurrent" and parser[len(parser)-1]=="Execution"):
            concurrent(count, workflow_list, time, f)
        count = count+1
    for i in range(0,len(workflow_list)):
        parser = workflow_list[i][0].split('.')
        if(parser[1] == "Activities" and parser[len(parser)-1]=="Type" and workflow_list[i][1]=="Flow"):
            t = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            timestamp = t + ";"
            timestamp = timestamp + parser[0] + "." + parser[len(parser)-2] + " Exit\n"
            f.write(timestamp)
    timestamp = ''  
    timestamp += timestamp + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    timestamp += ';'
    key = data.keys()
    for k in key:
        timestamp += k
        timestamp += " Exit\n"
    f.write(timestamp)  
    
    f.close()
print_log()