from __future__ import absolute_import
import os
import sys
import json
import time
import queue as q
import psutil
import argparse
import traceback
import subprocess
from enum import Enum
from subprocess import Popen
from lcdk import lcdk as LeslieChow

class Status(Enum):
    invalid = -2
    crashed = -1
    created = 0
    running= 1
    complete = 2

class ProcessQueue:
    def __init__(self, **kwargs):
        self.q_name = kwargs.get("name","")
        self.qsize = kwargs.get("size", 0)
        self.pids = []
        self.processQ = q.Queue(maxsize=self.qsize)
        self.processQ.processQueueName  = self.q_name



class Process: 
    def __init__(self): 
        self.name = ""
        self.status = Status(0)
        self.create_start = time.time()
        self.create_end = -1
        self.run_start = -1
        self.run_end = -1
        self.complete_time = -1
        self.crash_start = -1
        self.crash_end = -1
        self._process = None
        self.pid = -1
        self.launch_line = ""

           


class CrawlManager:
    def __init__(self,**kwargs):
        
        self.DBG = LeslieChow.lcdk()
        self.status = Status(-2)
        max_procs = kwargs.get('max_procs', 4)

        self.crashed = ProcessQueue(name="crashed")
        self.created = ProcessQueue(name="created")
        self.running = ProcessQueue(name="running", size=max_procs)
        self.completed = ProcessQueue(name="completed")
        self.monitoring = False
        self.q_ = dict()
        self.q_["crashed"] = self.crashed
        self.q_["created"] = self.created 
        self.q_["running"] =  self.running
        self.q_["completed"] =  self.completed
        self.proc_info = ['cmdline',
                          'connections'                  
                          'cpu_affinity',
                          'cpu_num',
                          'cpu_percent',
                          'cpu_times',
                          'create_time',
                          'cwd',
                          'environ',
                          'exe',
                          'gids',
                          'io_counters',
                          'ionice',
                          'memory_full_info',
                          'memory_info',
                          'memory_maps',
                          'memory_percent',
                          'name',
                          'nice',
                          'num_ctx_switches',
                          'num_fds',
                          'num_handles',
                          'num_threads',
                          'open_files',
                          'pid',
                          'ppid',
                          'status',
                          'terminal',
                          'threads',
                          'uids',
                          'username']


    def create_process(self, name, launch_line):
        p = Process()
        now = time.time()
        p = self.update_process_attributes(p, name=name, create_start=now, launch_line=launch_line)
        return p

    def deque_process(self,qName): 
        p = {}
        try: 
            if qName == "crashed": 
                if not self.crashed.processQ.empty():
                    p = self.crashed.processQ.get()
            elif qName == "created": 
                if not self.created.processQ.empty():
                    p = self.created.processQ.get()
            elif qName == "running": 
                if not self.running.processQ.empty():
                    p = self.running.processQ.get()
            elif qName == "completed": 
                if not self.completed.processQ.empty():
                    p = self.completed.processQ.get()
            else: 
                raise Exception("Invalid queue name {}".format(qName))


        except Exception as errorMsg: 
            self.DBG.error(str(errorMsg))   
        try:  
            if not p: 
                errorMsg = Exception("Queue {} is empty".format(qName)) 
        except Exception as errorMsg: 
            self.DBG.error(str(errorMsg)) 
        return p

        

    def enque_process(self, process): 
        try: 
            name = "{}:{}".format(process.pid, process.name)
            proc = {"name": name, "process": process}
            if process.status == self.status.crashed:
                self.crashed.processQ.put(proc)
            elif process.status == self.status.created: 
                self.created.processQ.put(proc)
            elif process.status == self.status.running:
                self.running.processQ.put(proc)
            elif process.status== self.status.complete: 
                self.completed.processQ.put(proc)
            else: 
                raise Exception("Process container {} does not exist".format(process.name))
                
        except Exception as errorMsg: 
            errorMsg = str(errorMsg)
            self.DBG.error(str(errorMsg))
    
    def update_process_attributes(self, p, **kwargs):
        """ 
        update_process_attributes: 
        Args: 
            p (:obj:`Process`): A process from the crawlManager module Process class
            name (:obj:`str`, optional): update the process name attribute
            status (:obj:`Status`, optional): update the process status attribute
            create_start (:obj:`float`, optional): update the process create_time attribute to epoch time 
            run_start (:obj:`float`, optional):  update the process run_start attribute to epoch time 
            run_end (:obj:`float`, optional):  update the process run_end attribute to epoch time 
            complete_time (:obj:`float`, optional):  update the process create_time attribute to epoch time 
            pid (:obj:`int`, optional): updates the process pid attribute
            process (:obj:`Popen`, optional): updates the process _process attibute
            launch_line (:obj:`str`, optional): updates the launch_line attribute 
        
        Examples:
            This function must be called by a crawlManager object. 

            cm  = CrawlManager()
            p = Process()
            pobj = Popen(...)
            cm.update_process_attributes(p, 
                                         name='test.py proccess', 
                                         status=Status().running, 
                                         create_start=time.time()
                                         pid=pobj.pid
                                         process=pobj
                                         launch_line="python test.py" 
                                         )

        Returns:
            An `Process` object with updated attributes
        """
        p.name = kwargs.get('name', p.name)
        p.status = kwargs.get('status', p.status)
        p.create_time = kwargs.get('create_time', p.create_start)
        p.run_start = kwargs.get('run_start',p.run_start)
        p.run_end = kwargs.get('run_end', p.run_end)
        p.complete_time = kwargs.get('complete_time', p.complete_time)
        p.pid = kwargs.get('pid', p.pid)
        p._process = kwargs.get('process', p._process)
        p.launch_line = kwargs.get('launch_line', p.launch_line)

        return p

    def launch_process(self, p, *args, **kwargs):

        try: 
            p_obj =  subprocess.Popen(p.launch_line)
            out, errs = p_obj.communicate(timeout=15)
            self.running.pids.append(p_obj.pid)
            self.DBG.warning(out)
            self.DBG.warning(errs)

            p = self.update_process_attributes(p, process=p_obj, pid=p_obj.pid)
        except Exception as errorMsg:
            self.DBG.error(str(errorMsg))
        return p
            
    def stop_monitoring(self):
        self.monitoring = False

    def execute(self, process_name, launch_line):
        p = self.create_process(name=process_name, launch_line=launch_line)
        self.enque_process(p)


    def swap_queues(self, srcQ='created', destQ="created"): 
        self.DBG.log("{} empty: {}".format(srcQ, self.q_[srcQ].processQ.empty()))
        
        proc = self.deque_process(qName=srcQ)

        if not proc: 
            return
        p = proc['process']
        p.name = proc['name']
        try: 
            if srcQ == "crashed":
                p.crash_end = time.time()

            if srcQ == "created" and destQ != "created":
                p = self.update_process_attributes(p, create_end=time.time())

            if srcQ == "running": 
                p.run_end = time.time()

            if destQ == 'crashed':
                status = self.status.crashed
                crashed_start = time.time()
                p = self.update_process_attributes(p, crashed_start=crashed_start, status=status)
                self.enque_process(p)
                
            if destQ == 'created':
                status = self.status.created
                create_start = time.time()
                p = self.update_process_attributes(p, create_start=create_start, status=status)
                self.enque_process(p)

            if destQ == 'running':
                status = self.status.running
                run_start = time.time()
                if not self.running.processQ.full(): 
                    p = self.update_process_attributes(p, run_start=run_start, status=status)
                    self.enque_process(p)
                    self.launch_process(p)
                else: 
                    raise Exception("Queue 'running' is full")

            if destQ == 'completed':
                status = self.status.complete
                complete_time = time.time()
                p = self.update_process_attributes(p, complete_time=complete_time, status=status)
                self.enque_process(p)

        except Exception as errorMsg:
            self.DBG.error(str(errorMsg))
    
    def _print_queues_sizes(self):
        for qname in self.q_:
            q = self.q_[qname].processQ
            msg = "{}: {}".format(qname, q.qsize())
            self.DBG.warning(msg)

    def start_monitoring(self):
        self.monitoring = True
        while self.monitoring:
            running_procs = []
            pinfo = ""
            proc_info = []

            for proc in psutil.process_iter():

                try:
                    pinfo = proc.as_dict(attrs=proc_info)
                    name = "{}".format(pinfo['pid'])
                    running_procs.append(name)
                except:  
                    pass
                
            for pid in self.running.pids: 
                self.DBG.warning("{},{}".format(pid, pid in running_procs))
                if pid not in running_procs:
                    
                    self.swap_queues("running", "completed")
            self.swap_queues("created", "running")

            
            all_empty = True
            
            for qname in self.q_:
                if qname == "completed": 
                    continue
                q = self.q_[qname]
                print(qname, q.processQ.empty())

                all_empty = all_empty and q.processQ.empty()
            print(all_empty)
            print('\n\n')
            self._print_queues_sizes()
            if all_empty:
                break
        
                    

if __name__ == "__main__":
    cm  = CrawlManager()
    parser = argparse.ArgumentParser()
    parser.add_argument('-bin_cmd', help='binary to run')
    parser.add_argument('--bin_args', help='binary arguments')
    parser.add_argument('--prog_args', type=str, nargs='+', help='<Required> Set flag')
    
    args = parser.parse_args()
    
    bin_cmd = args.bin_cmd
    bin_args = args.bin_args
    prog_args = args.prog_args
    print(args.bin_args, args.prog_args)
    for index in prog_args: 
        launch_line = ['{}'.format(bin_cmd),
                       '{}'.format(bin_args), 
                       "--index",
                       '{}'.format(index)
                      ]
        name = "".join(launch_line)
        print(launch_line)
        cm.execute(name, launch_line)
    cm.start_monitoring()
        





    