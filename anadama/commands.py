import sys
import six
import codecs
import inspect
import pprint
from cStringIO import StringIO
from functools import partial
from operator import attrgetter

from doit.cmd_run import Run as DoitRun
from doit.cmd_run import (
    opt_always, opt_continue, opt_verbosity, 
    opt_reporter, opt_num_process, opt_single,
)
from doit.cmd_help import Help as DoitHelp
from doit.cmd_list import List as DoitList
from doit.task import Task
from doit.control import TaskControl
from doit.runner import Runner, MRunner, MThreadRunner
from doit.cmd_base import DoitCmdBase, Command
from doit.cmdparse import CmdOption
from doit.exceptions import InvalidCommand, InvalidDodoFile

from . import dag
from .runner import PAR_TYPES
from .reporter import REPORTERS
from .provenance import find_versions
from .loader import PipelineLoader

opt_tmpfiles = dict(
    name    = "tmpfiledir",
    long    = "tmpfiledir",
    default = "/tmp",
    help    = "Where to save temporary files",
    type    = str
)

opt_pipeline_name = dict(
    name    = "pipeline_name",
    long    = "pipeline_name",
    default = "Custom Pipeline",
    help    = "Optional name to give to the current pipeline",
    type    = str
)

opt_reporter['type'] = REPORTERS
opt_reporter['help'] = \
"""Choose output reporter. Available:
'default': report output on console
'executed-only': no output for skipped (up-to-date) and group tasks
'json': output result in json format
'verbose': output actions on console as they're executed
[default: %(default)s]
"""

class AnadamaCmdBase(DoitCmdBase):
    my_base_opts = ()
    my_opts = ()

    def set_options(self):
        opt_list = (self.my_base_opts + self.my_opts + 
                    self.base_options + self._loader.cmd_options +
                    self.cmd_options)
        return [CmdOption(opt) for opt in opt_list]



class ListDag(AnadamaCmdBase, DoitRun):
    my_opts = (opt_tmpfiles, opt_pipeline_name)
    name = "dag"
    doc_purpose = "print execution tree"
    doc_usage = "[TASK ...]"


    def _execute(self, verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0, single=False,
                 pipeline_name="Custom Pipeline", tmpfiledir="/tmp",
                 **kwargs):
        # **kwargs are thrown away
        dag.TMP_FILE_DIR = tmpfiledir
        runner = PAR_TYPES['jenkins']
        runner.pipeline_name = pipeline_name
        return super(ListDag, self)._execute(outfile=sys.stdout,
                                             verbosity=verbosity,
                                             always=always,
                                             continue_=continue_,
                                             reporter=reporter,
                                             num_process=1,
                                             par_type=runner,
                                             single=single)


class Help(DoitHelp):
    name = "help"

    @staticmethod
    def print_usage(cmds):
        """Print anadama usage instructions"""
        print("AnADAMA -- https://bitbucket.org/biobakery/anadama")
        print('')
        print("Commands")
        for cmd in sorted(six.itervalues(cmds), key=attrgetter('name')):
            six.print_("  anadama %s \t\t %s" % (cmd.name, cmd.doc_purpose))
        print("")
        print("  anadama help                              show help / reference")
        print("  anadama help task                         show help on task fields")
        print("  anadama help pipeline <module:Pipeline>   show module.Pipeline help")
        print("  anadama help <command>                    show command usage")
        print("  anadama help <task-name>                  show task usage")


    @staticmethod
    def print_pipeline_help(pipeline_class):
        message = StringIO()
        spec = inspect.getargspec(pipeline_class.__init__)
        args = [a for a in spec.args if a != "self"] #filter out self
        print >> message, "Arguments: "
        print >> message, pprint.pformat(args)

        print >> message, "Default options: "
        print >> message, pprint.pformat(pipeline_class.default_options)

        print >> message, "" #newline
        print >> message, pipeline_class.__doc__

        print >> message, ""
        print >> message, pipeline_class.__init__.__doc__
        
        print message.getvalue()


    def execute(self, params, args):
        """execute cmd 'help' """
        cmds = self.doit_app.sub_cmds
        if len(args) == 0 or len(args) > 2:
            self.print_usage(cmds)
        elif args[0] == 'task':
            self.print_task_help()
        elif args == ['pipeline']:
            six.print_(cmds['pipeline'].help())
        elif args[0] == 'pipeline':
            cls = PipelineLoader._import(args[1])
            self.print_pipeline_help(cls)
        elif args[0] in cmds:
            # help on command
            six.print_(cmds[args[0]].help())
        else:
            # help of specific task
            try:
                if not DoitCmdBase.execute(self, params, args):
                    self.print_usage(cmds)
            except InvalidDodoFile as e:
                self.print_usage(cmds)
                raise InvalidCommand("Unable to retrieve task help: "+e.message)
        return 0



class BinaryProvenance(Command):
    name = "binary_provenance"
    doc_purpose = "print versions for required dependencies"
    doc_usage = "<module> [<module> [<module...]]"
    
    def execute(self, opt_values, pos_args):
        """Import workflow modules as specified from positional arguments and
        determine versions of installed executables and other
        dependencies via py:ref:`provenance.find_versions`.

        For each external dependency installed, print the name of the
        dependency and the version of the dependency.

        """

        for mod_name in pos_args:
            for binary, version in find_versions(mod_name):
                print binary, "\t", version


class RunPipeline(DoitRun):
    name = "pipeline"
    doc_purpose = "run an AnADAMA pipeline"
    doc_usage = "<module.Pipeline> [options]"

    cmd_options = (opt_always, opt_continue, opt_verbosity, 
                   opt_reporter, opt_num_process, opt_single)

    my_opts = (opt_tmpfiles, opt_pipeline_name)

    def __init__(self, *args, **kwargs):
        kwargs['task_loader'] = PipelineLoader()
        super(RunPipeline, self).__init__(*args, **kwargs)


    def parse_execute(self, in_args):
        if not in_args:
            raise InvalidCommand("No pipeline specified. Try pipeline -h")
        pipeline_name, in_args = in_args[0], in_args[1:]
        self._loader.pipeline_cls = pipeline_name
        return super(RunPipeline, self).parse_execute(in_args)
        

    def _execute(self, verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0, single=False, 
                 pipeline_name="Custom Pipeline", 
                 **kwargs):
        # **kwargs are thrown away
        return super(RunPipeline, self)._execute(outfile=sys.stdout,
                                                 verbosity=verbosity,
                                                 always=always,
                                                 continue_=continue_,
                                                 reporter=reporter,
                                                 num_process=num_process,
                                                 par_type=PAR_TYPES['process'],
                                                 single=single)
        


class DagPipeline(RunPipeline, ListDag):
    name = "pipeline_dag"
    doc_purpose = "print dag from pipeline"
    doc_usage = "<some_module.SomePipeline> [options]"



all = (ListDag, Help, BinaryProvenance, RunPipeline, DagPipeline)
