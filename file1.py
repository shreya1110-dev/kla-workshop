#!/usr/bin/env python

from typing import MutableMapping
import yaml
from datetime import datetime
import time
import pandas as pd
from collections import MutableMapping

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

def print_log():
    data = read_yaml()
    workflow = read_dict(data)
    timestamp = ''
    timestamp += timestamp + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    timestamp += ','
    key = data.keys()
    for k in key:
        timestamp += k
        timestamp += " Entry"
    f = open("file1.txt", "a")
    f.write(timestamp)
    for k,v in workflow.items():
        parser = k.split('.')
        if(parser[len(parser)-1] == "Execution"):
            if(workflow[k] == "")

print_log()