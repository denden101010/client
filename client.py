# -*- coding: utf-8 -*-
"""
Created on Fri Aug 20 23:33:51 2021

@author: duohu
"""

from asyncio import tasks
import json
import requests
import csv
import time;
import queue
from multiprocessing import Lock, Pool
from requests.api import head

import threading

from requests.sessions import Session

session = Session()
lock = threading.Lock()
locks = {}

host = "http://94.228.121.219:8080"

def load_url(url, data):
    text = json.dumps(data)
    response = session.post(url, data = text, headers = {'Content-Length': str(len(text))}, timeout=5)
    data = json.loads(response.text)
    return data

def run_task(task):
    url = task["task"]["url"]
    text = session.get(url)
    if text.status_code != 200:
        return []
    dict = {}
    dict["id"] = task["id"]
    dict["task_id"] = task["task_id"]
    dict["body"] = text.content.decode("utf-8")
    if "delay" in task:
        time.sleep(task["delay"])
    return dict

delay_pools = {}

def run_tasks(tasks, pool):

    data_array = []
    results = []
    
    for task in tasks:
        try:
            if "delay" in task:
                if task["task_id"] not in delay_pools:
                    delay_pools[task["task_id"]] = Pool(1)
                result = delay_pools[task["task_id"]].apply_async(run_task, [task])
                results.append(result)
            else:
                result = pool.apply_async(run_task, [task])
                results.append(result)
        except Exception:
            continue
    i = 1
    for result in results:
        data = result.get()
        if len(data) != 0:
            data_array.append(data)
        print(i , "/", len(tasks))
        i += 1
        
    return {"tasks":data_array}
    

sleep = 5

def loop(pool):
    while True:
        st = time.time()
        skipslip = False
        try:
            json = load_url(host + "/getTasks", { "ok":"ok" })
            if "tasks" in json:
                if len(json["tasks"]) != 0:
                    skipslip = True
                result = run_tasks(json["tasks"], pool)
                if len(result["tasks"]) != 0:
                    load_url(host + "/pushResult",result)
        except Exception:
            pass
        if skipslip:
            continue
        delay = sleep - (time.time() - st)
        if delay > 0:
            time.sleep(delay)

#Основная программа
if __name__ == '__main__':
    pool = Pool(10)
    loop(pool)