from functools import partial
from collections import defaultdict

from doit.reporter import ConsoleReporter
from doit.reporter import REPORTERS as doit_REPORTERS
from doit.action import CmdAction

class VerboseConsoleReporter(ConsoleReporter):

    def __init__(self, *args, **kwargs):
        super(VerboseConsoleReporter, self).__init__(*args, **kwargs)
        self._open_files = defaultdict(list)

    
    def _open_log(self, task, action):
        if not task.targets:
            return
        task_name = task.targets[0]+".log"
        outfile = open(task_name, 'w')
        self._open_files[task.name].append(outfile)
        action.execute = partial(action.execute, out=outfile, err=outfile)

        
    def _close_log(self, task, action):
        for f in self._open_files[task.name]:
            f.close()

            
    def execute_task(self, task, *args, **kwargs):
        super(VerboseConsoleReporter, self).execute_task(task, *args, **kwargs)
        if task.actions and (task.name[0] != '_'):
            for action in task.actions:
                if hasattr(action, 'expand_action'):
                    # I don't think calling expand_action has any side
                    # effects right now. hope it never does!
                    self.write(action.expand_action()+'\n')
                self._add_log(task, action)


    def teardown_task(self, task):
        for action in task.actions:
            self._close_log(task, action)



REPORTERS = doit_REPORTERS
REPORTERS['verbose'] = VerboseConsoleReporter
