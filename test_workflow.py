#! /usr/bin/env python

import configobj
import os
import os.path
import sqlalchemy as sqla
import sys

## Interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
from gc3libs.persistence.accessors import GetValue
import gc3libs.utils

config_file = '/Users/elkeschaper/Python_projects/Gc3pie_error_prone_workflow/test_defaults.ini'
c = configobj.ConfigObj(config_file, stringify=True)
#c = configobj.ConfigObj(config_file, configspec = config_specs, stringify=True)


############################# Basic Applications/Tasks ###################################


class A(Application):
    def __init__(self, jokes, **kwargs):

        gc3libs.log.info("Initialising A")

        gc3libs.Application.__init__(self,
                                     arguments = [c['script_A'], "-i", c['input_A'], "-o", c['output_A']],
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = c['stdout_name'],
                                     stderr = c['stderr_name'],
                                     output_dir = c['output_A'],
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("Testing whether A is done")

        # If the application has terminated o.k. (self.execution.returncode == 0),
        #   Check wether all resultfiles are o.k. If yes: Good, If no: FREEZE.
        # If not: Save error and FREEZE.

        if self.execution.returncode == 0:
            gc3libs.log.info("A claims to be successful: self.execution.returncode: {}".format(self.execution.returncode))
            # Check if result file exists (Later: and complies to some rules).
            if not os.path.isfile(c['output_A']):
                gc3libs.log.info("A has not produced an output file.")
                self.execution.returncode = 1
                self.status = "FREEZE"

            else:
                gc3libs.log.info("A has run successfully to completion.")
                # Now, clean up
                # Delete all log files.
                os.remove(os.path.join(self.output_dir, c['stdout_name']))
                os.remove(os.path.join(self.output_dir, c['stderr_name']))

        else:
            gc3libs.log.info("A is not successful: self.execution.returncode: {}".format(self.execution.returncode))
            # Check if there is stderr.
            if not os.path.isfile(c['stderr_name']):
                self.error_tag = ""
            else:
                # Create a tag from the last line in stderr.
                with open(os.path.join(self.output_directory, self.stderr), "r") as fh:
                    for line in fh:
                        pass
                    self.error_tag = line

        self.status = "FREEZE"

        #self.execution.state in [TERMINATED, RUNNING, STOPPED, SUBMITTED.]


class B(Application):
    def __init__(self, joke, **kwargs):

        gc3libs.log.info("B")

        gc3libs.Application.__init__(self,
                                     arguments = [c['script_B'], "-i", c['output_A'], "-o", c['output_B']],
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = "stdout.log",
                                     output_dir = c['output_B'],
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("B is done.")





class C(Application):
    def __init__(self, joke, **kwargs):

        gc3libs.log.info("C")

        gc3libs.Application.__init__(self,
                                     arguments = ["/bin/hostname"],
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = "stdout.log",
                                     output_dir = "./results/C_{}".format(joke),
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("C is done.")


class D(Application):
    def __init__(self, joke, **kwargs):

        gc3libs.log.info("D")

        gc3libs.Application.__init__(self,
                                     arguments = ["/bin/ps"],
                                     inputs = [],
                                     outputs = [],
                                     join = True,
                                     stdout = "stdout.log",
                                     output_dir = "./results/D_{}".format(joke),
                                     **kwargs
                                     )

    def terminated(self):
        gc3libs.log.info("D is done.")


############################# Main Session Creator (?) ###################################


class TestWorkflow(SessionBasedScript):
    """
    Test pipeline
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.0.1',
            )

    def setup_options(self):
        self.add_param("-j", "--jokes", type=str, nargs="+",
                       help="List of jokes")

    def _make_session(self, session_uri, store_url):
        return gc3libs.session.Session(
            session_uri,
            store_url,
            extra_fields = {
                # NB: enlarge window to at least 150 columns to read this table properly!
                sqla.Column('class',              sqla.TEXT())    : (lambda obj: obj.__class__.__name__)                                              , # task class
                sqla.Column('name',               sqla.TEXT())    : GetValue()             .jobname                                                   , # job name
                sqla.Column('executable',         sqla.TEXT())    : GetValue(default=None) .arguments[0]                        ,#.ONLY(CodemlApplication), # program executable
                sqla.Column('output_path',        sqla.TEXT())    : GetValue(default=None) .output_dir                        ,#.ONLY(CodemlApplication), # fullpath to codeml output directory
                sqla.Column('cluster',            sqla.TEXT())    : GetValue(default=None) .execution.resource_name           ,#.ONLY(CodemlApplication), # cluster/compute element
                sqla.Column('worker',             sqla.TEXT())    : GetValue(default=None) .hostname                          ,#.ONLY(CodemlApplication), # hostname of the worker node
                sqla.Column('cpu',                sqla.TEXT())    : GetValue(default=None) .cpuinfo                           ,#.ONLY(CodemlApplication), # CPU model of the worker node
                sqla.Column('requested_walltime', sqla.INTEGER()) : _get_requested_walltime_or_none                           , # requested walltime, in hours
                sqla.Column('requested_cores',    sqla.INTEGER()) : GetValue(default=None) .requested_cores                   ,#.ONLY(CodemlApplication), # num of cores requested
                sqla.Column('used_walltime',      sqla.INTEGER()) : GetValue(default=None) .execution.used_walltime           ,#.ONLY(CodemlApplication), # used walltime
                sqla.Column('lrms_jobid',         sqla.TEXT())    : GetValue(default=None) .execution.lrms_jobid              ,#.ONLY(CodemlApplication), # arc job ID
                sqla.Column('original_exitcode',  sqla.INTEGER()) : GetValue(default=None) .execution.original_exitcode       ,#.ONLY(CodemlApplication), # original exitcode
                sqla.Column('used_cputime',       sqla.INTEGER()) : GetValue(default=None) .execution.used_cputime            ,#.ONLY(CodemlApplication), # used cputime in sec
                # returncode = exitcode*256 + signal
                sqla.Column('returncode',         sqla.INTEGER()) : GetValue(default=None) .execution.returncode              ,#.ONLY(CodemlApplication), # returncode attr
                sqla.Column('queue',              sqla.TEXT())    : GetValue(default=None) .execution.queue                   ,#.ONLY(CodemlApplication), # exec queue _name_
                sqla.Column('time_submitted',     sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['SUBMITTED']  ,#.ONLY(CodemlApplication), # client-side submission (float) time
                sqla.Column('time_terminated',    sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['TERMINATED'] ,#.ONLY(CodemlApplication), # client-side termination (float) time
                sqla.Column('time_stopped',       sqla.FLOAT())   : GetValue(default=None) .execution.timestamp['STOPPED']    ,#.ONLY(CodemlApplication), # client-side stop (float) time
                sqla.Column('error_tag',          sqla.TEXT())    : GetValue(default=None) .error_tag
                })

    def parse_args(self):
        self.jokes = self.params.jokes
        gc3libs.log.info("TestWorkflow Jokes: {}".format(self.jokes))


    def new_tasks(self, kwargs):

        #name = "myTestWorkflow"
        gc3libs.log.info("Calling TestWorkflow.next_tasks()")

        yield test_workflow.MainSequentialFlow(self.jokes, **kwargs)


######################## Support Classes / Workflow elements #############################


class MainSequentialFlow(SequentialTaskCollection):
    def __init__(self, jokes, **kwargs):
        self.jokes = jokes

        gc3libs.log.info("\t Calling MainSequentialFlow.__init({})".format(jokes))

        self.initial_task = A(self.jokes)

        SequentialTaskCollection.__init__(self, [self.initial_task], **kwargs)

    def next(self, iterator):
        if iterator == 0:
            self.add(MainParallelFlow(self.jokes))
            return Run.State.RUNNING
        elif iterator == 1:
            self.add(D(self.jokes))
            return Run.State.RUNNING
        else:
            return Run.State.TERMINATED

    def terminated(self):
        gc3libs.log.info("\t MainSequentialFlow.terminated [%s]" % self.execution.returncode)


class MainParallelFlow(ParallelTaskCollection):

    def __init__(self, jokes, **kwargs):

        self.jokes = jokes
        gc3libs.log.info("\t\tCalling MainParallelFlow.__init({})".format(self.jokes))

        self.tasks = [InnerSequentialFlow(joke) for joke in self.jokes]

        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tMainParallelFlow.terminated")



class InnerSequentialFlow(SequentialTaskCollection):
    def __init__(self, joke, **kwargs):

        self.joke = joke
        gc3libs.log.info("\t\t\t\tCalling InnerSequentialFlow.__init__ for joke: {}".format(self.joke))

        self.job_name = joke
        initial_task = B(self.joke)
        SequentialTaskCollection.__init__(self, [initial_task], **kwargs)

    def next(self, iterator):
        if iterator == 0:
            gc3libs.log.info("\t\t\t\tCalling InnerSequentialFlow.next(%d) ... " % int(iterator))
            self.add(C(self.joke))
            return Run.State.RUNNING
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED

    def terminated(self):
        gc3libs.log.info("\t\t\t\tInnerSequentialFlow.terminated [%d]" % self.execution.returncode)


def _get_requested_walltime_or_none(job):
    if isinstance(job, gc3libs.application.codeml.CodemlApplication):
        return job.requested_walltime.amount(hours)
    else:
        return None


# run script
if __name__ == '__main__':
    import test_workflow
    TestWorkflow().run()