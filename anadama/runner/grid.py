import os
import re
import time
import tempfile
import operator
import itertools
import subprocess
from math import exp

from doit.exceptions import CatchedException
from doit.runner import MThreadRunner

from .. import picklerunner, performance
from ..util import dict_to_cmd_opts, partition, intatleast1


sigmoid = lambda t: 1/(1-exp(-t))
first = operator.itemgetter(0)


class GridRunner(MThreadRunner):
    def __init__(self, partition,
                 performance_url=None,
                 tmpdir="/tmp",
                 extra_grid_args="",
                 *args, **kwargs):
        super(GridRunner, self).__init__(*args, **kwargs)
        self.partition = partition
        self.tmpdir = tmpdir
        self.performance_predictor = performance.new_predictor(performance_url)
        self.extra_grid_args = extra_grid_args
        self.id_task_map = dict()


    def execute_task(self, task):
        perf = self.performance_predictor.predict(task)
        self.reporter.execute_task(task)
        if not task.actions:
            return None

        maybe_exc, task_id = self._grid_execute_task(task, perf)
        if task_id:
            self.id_task_map[task_id] = task

        return maybe_exc


    def finish(self):
        for task, (max_rss_mb, cpu_hrs, clock_hrs) in self._grid_summarize():
            self.performance_predictor.update(task, max_rss_mb,
                                              cpu_hrs, clock_hrs)
        self.performance_predictor.save()
        return super(GridRunner, self).finish()
    

    def _grid_execute_task(self, task, perf_obj):
        keep_going, maybe_exc, tries = True, None, 1
        mem, time, threads = map(intatleast1, perf_obj)
        task_id = None
        while keep_going:
            keep_going, maybe_exc = False, None
            cmd, (out, err, retcode) = self._grid_communicate(
                task, self.partition, 
                mem, time, 
                threads=threads, 
                tmpdir=self.tmpdir,
                extra_grid_args=self.extra_grid_args)
            if retcode:
                packed = self._handle_grid_fail(cmd, out, err,
                                                retcode, tries, mem, time)
                maybe_exc, keep_going, mem, time = packed
            else:
                task_id = self._find_job_id(out, err)
        return maybe_exc, task_id


    @staticmethod
    def _grid_popen(cmd, task):
        proc = subprocess.Popen([cmd], shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if not task.actions[0].out:
            task.actions[0].out = str()
        if not task.actions[0].err:
            task.actions[0].err = str()
        task.actions[0].out += out
        task.actions[0].err += err

        if proc.returncode == 0:
            for _ in range(3):
                if all(map(os.path.exists, task.targets)):
                    break
                time.sleep(1)

        return out, err, proc.returncode


    def _grid_summarize(self):
        for chunk in partition(self.id_task_map.iteritems(), 100):
            ids, tasks = zip(*sorted(filter(bool, chunk), key=first))
            stats = sorted(self._jobstats(ids), key=first)
            for task, stat in zip(tasks, stats):
                yield task, stat

    # You'll have to implement the below methods yourself to make your
    # own grid runner

    @staticmethod
    def _grid_communicate(task, partition, mem, time, 
                          tmpdir='/tmp', threads=1, extra_grid_args=""):
        raise NotImplementedError()


    def _find_job_id(self, out, err):
        raise NotImplementedError()

    def _jobstats(self, ids):
        raise NotImplementedError()

    @staticmethod
    def _handle_grid_fail(cmd, out, err, retcode, tries, mem, time):
        raise NotImplementedError()


class DummyGridRunner(GridRunner):
    def __init__(self, *args, **kwargs):
        self.task_id_counter = itertools.count(1)
        self.task_performance_info = dict()
        return super(DummyGridRunner, self).__init__(*args, **kwargs)


    @staticmethod
    def _grid_communicate(task, partition, mem, time,
                          tmpdir="/tmp", threads=1, extra_grid_args=""):
        cmd = ( "/usr/bin/time -f 'TASK_PERFORMANCE %e %M %S %U' "
                +picklerunner.tmp(task, dir=tmpdir).path+" -r" )
        return cmd, DummyGridRunner._grid_popen(cmd, task)


    def _find_job_id(self, out, err):
        id = next(self.task_id_counter)
        c_sec, mem_k, k_sec, u_sec = map(float, re.search(
            r'TASK_PERFORMANCE ([\d.]+) ([\d.]+) ([\d.]+) ([\d.]+)',
            err).groups())
        cpu_hrs = (k_sec+u_sec)/3600
        self.task_performance_info[id] = (mem_k/1024, cpu_hrs, c_sec/3600)
        return id


    def _jobstats(self, ids):
        return map(self.task_performance_info.__getitem__, ids)


    @staticmethod
    def _handle_grid_fail(cmd, out, err, retcode, tries, mem, time):
        exc = CatchedException("Command failed: "+cmd+"\n"+out+"\n"+err)
        keep_going = False
        return exc, keep_going, int(mem), int(time)


class SlurmRunner(GridRunner):
    @staticmethod
    def _grid_communicate(task, partition, mem, time,
                          tmpdir="/tmp", threads=1, extra_grid_args=""):
        opts = { "mem": mem,   
                 "time": time,
                 "export": "ALL", 
                 "partition": partition,
                 "cpus-per-task": threads }

        cmd = ( "srun -v "
                +" "+dict_to_cmd_opts(opts)
                +" "+extra_grid_args+" "
                +" "+picklerunner.tmp(task, dir=tmpdir).path+" -r" )

        return cmd, SlurmRunner._grid_popen(cmd, task)


    @staticmethod
    def _find_job_id(out, err):
        return re.search(r'launching (\d+).(\d+) on host',
                         err).group(1)


    @staticmethod
    def _jobstats(ids):
        ids = ",".join(ids)
        def _fields():
            proc = subprocess.Popen(
                ["sacct",
                 "--format", "MaxRSS,TotalCPU,Elapsed,ExitCode,State",
                 "-P", "-j", ids],
                stdout=subprocess.PIPE)
            for line in proc.stdout:
                fields = line.strip().split("|")
                if any((fields[-1] != "COMPLETED", fields[4] != "0:0")):
                    continue
                yield fields[:-2]
            proc.wait()

        for rss, cputime, clocktime in _fields():
            rss = float(rss.replace("K", ""))/1024,
            clockparts = map(float, clocktime.split(":"))
            clocktime = clockparts[0] + clockparts[1]/60 + clockparts[2]/3600
            cpuparts = map(float, cputime.split(":"))
            cputime = cpuparts[0] + cpuparts[1]/60 + cpuparts[2]/3600
            yield rss, cputime, clocktime
                    

    @staticmethod
    def _handle_grid_fail(cmd, out, err, retcode, tries, mem, time):
        exc = CatchedException("srun command failed: "+cmd
                               +"\n"+out+"\n"+err)
        keep_going = False
        outerr = out+err
        if "Exceeded job memory limit" in outerr:
            used = re.search(r'memory limit \((\d+) > \d+\)', outerr).group(1)
            mem = int(used)/1024 * (1.3**tries)
            keep_going = True
        if re.search(r"due to time limit", outerr, re.IGNORECASE):
            time = time * (sigmoid(tries/10.)*2.7)
            keep_going = True

        return exc, keep_going, int(mem), int(time)



class LSFRunner(GridRunner):
    fmt = ('cpu_used max_mem run_time exit_code'
           ' exit_reason stat delimiter="|"')
    multipliers = {
        "gbytes": lambda f: f/1024,
        "mbytes": lambda f: f,
        "kbytes": lambda f: f*1024
    }


    @staticmethod
    def _grid_communicate(task, partition, mem, time, 
                          tmpdir='/tmp', threads=1, extra_grid_args=""):
        rusage = "span[hosts=1] rusage[mem={}:duration={}]".format(
            mem, int(time))
        tmpout = tempfile.mktemp(dir=tmpdir)
        opts ={ 'R': rusage, 'o': tmpout,
                'n': threads,'q': partition }
        
        cmd = ( "bsub -K -r "
                +" "+dict_to_cmd_opts(opts)
                +" "+extra_grid_args+" "
                +" "+picklerunner.tmp(task, dir=tmpdir).path+" -r" )
        out, err, retcode = LSFRunner._grid_popen(cmd, task)

        try:
            with open(tmpout) as f:
                task.actions[0].err += f.read()
            os.unlink(tmpout)
        except Exception as e:
            err += "Anadama error: "+str(e)

        return cmd, (out, err, retcode)


    @staticmethod
    def _find_job_id(out, err):
        return re.search(r'Job <(\d+)>', out).group(1)


    @staticmethod
    def _jobstats(ids):
        def _fields():
            proc = subprocess.Popen(['bjobs', '-noheader',
                                     '-o '+LSFRunner.fmt]+list(ids),
                                    stdout=subprocess.PIPE)
            for line in proc.stdout:
                fields = line.strip().split("|")
                if any((fields[-1] != "DONE", 
                        fields[-2] != "-", 
                        fields[-3] != "-")):
                    continue
                yield fields[:3]

        for ctime, mem, wtime in _fields():
            key, mem_str = mem.split()
            mem = LSFRunner.multipliers[key](float(mem_str))
            clocktime = int(wtime.split()[0])
            cputime = int(ctime.split()[0])
            yield mem, cputime, clocktime


    @staticmethod
    def _handle_grid_fail(cmd, out, err, retcode, tries, mem, time):
        return None, False, mem, time



class SGERunner(GridRunner):
    useful_qacct_keys = ("mem", "cpu", "wallclock")

    def __init__(self, *args, **kwargs):
        self.task_performance_info = dict()
        self._pe_name = None
        return super(SGERunner, self).__init__(*args, **kwargs)

    def find_suitable_pe(self):
        if self._pe_name:
            return self._pe_name

        names, _ = subprocess.Popen(['qconf', '-spl'], 
                                    stdout=subprocess.PIPE).communicate()
        if not names:
            raise InvalidCommand(
                "Unable to find any SGE parallel environment names. \n"
                "Ensure that SGE tools like qconf are installed, \n"
                "ensure that this node can talk to the cluster, \n"
                "and ensure that parallel environments are enabled.")

        pe_name = None
        for name in names.strip().split():
            if pe_name:
                break
            out, _ = subprocess.Popen(["qconf", "-sp", name], 
                                   stdout=subprocess.PIPE).communicate()
            for line in out.split('\n'):
                if ["allocation_rule", "$pe_slots"] == line.split():
                    pe_name = name
        
        if not pe_name:
            raise InvalidCommand(
                "Unable to find a suitable parallel environment. "
                "Please talk with your systems administrator to enable "
                "a parallel environment that has an `allocation_rule` "
                "set to `$pe_slots`.")
        else:
            self._pe_name = pe_name
            return pe_name
            

    def _grid_communicate(self, task, partition, mem, time, 
                          tmpdir='/tmp', threads=1, extra_grid_args=""):
        pe_name = self.find_suitable_pe()
        mem = float(mem)/float(threads) # SGE spreads mem over requested num slots
        tmpout = tempfile.mktemp(dir=tmpdir)
        tmperr = tempfile.mktemp(dir=tmpdir)
        script = picklerunner.tmp(task, dir=tmpdir)

        cmd = ("qsub -R y -b y -sync y -pe {pe_name} {threads} -cwd "
               "-l 'm_mem_free={mem}M' -q {partition} -V "
               "-o {tmpout} -e {tmperr} "
               "{script} -r").format(pe_name=pe_name, threads=threads, 
                                     mem=max(1, int(mem)), partition=partition,
                                     tmpout=tmpout, tmperr=tmperr,
                                     script=script.path)

        out, err, retcode = SGERunner._grid_popen(cmd, task)
        
        try:
            if os.path.exists(tmpout):
                with open(tmpout) as f_out:
                    task.actions[0].out = f_out.read()
                os.unlink(tmpout)
            if os.path.exists(tmperr):
                with open(tmperr) as f_err:
                    task.actions[0].err = f_err.read()
                os.unlink(tmperr)
        except Exception as e:
            err += "Anadama error: "+str(e)

        return cmd, (out, err, retcode)


    @staticmethod
    def _find_job_id(out, err):
        return re.search(r'Your job (\d+) ', out).group(1)


    def _jobstats(self, ids):
        for job_id in ids:
            output = subprocess.Popen(
                ["qacct", "-j", str(job_id)], 
                stdout=subprocess.PIPE).communicate()[0]
            output = output.strip().split("\n")
            if not output:
                continue
            ret = dict([(k, 0) for k in self.useful_qacct_keys])
            for line in output:
                kv = line.split()
                if len(kv) < 2 or kv[0] not in ret:
                    continue
                k, v = kv
                ret[k] = float(v)

            yield map(ret.get, self.useful_qacct_keys)
                

    @staticmethod
    def _handle_grid_fail(cmd, out, err, retcode, tries, mem, time):
        exc = CatchedException("Command failed: "+cmd+"\n"+out+"\n"+err)
        keep_going = False
        return exc, keep_going, int(mem), int(time)

