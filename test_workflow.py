#! /usr/bin/env python

GLOBAL_OUTPUT_DIR = '/tmp'


import os
import os.path
import sys

## Interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

import gc3libs.debug


class StopOnError(object):
    """
    Mix-in class to make a `SequentialTaskCollection`:class: turn to STOPPED
    state as soon as one of the tasks fail.
    """
    def next(self, done):
        if done == len(self.tasks) - 1:
            self.execution.returncode = self.tasks[done].execution.returncode
            return Run.State.TERMINATED
        else:
            rc = self.tasks[done].execution.exitcode
            if rc != 0:
                return Run.State.STOPPED
            else:
                return Run.State.RUNNING


############################# Basic Applications/Tasks ###################################

class A(Application):
    @gc3libs.debug.trace
    def __init__(self, k, outdir, **kwargs):
        gc3libs.log.info("Creating A for producing %d output files", k)
        self.k = k
        self.outdir = outdir
        gc3libs.Application.__init__(
            self,
            arguments = [
                "./a2b.py",
                "-o", (outdir + '/A'),
                "-n", self.k,
            ],
            inputs = ['a2b.py'],
            outputs = [],
            #join = True,
            stdout = "stdout.log",
            stderr = "stderr.log",
            **kwargs
        )

    def terminated(self):
        # check that files `A.1`, `A.2`, etc. have been produced
        for j in range(self.k):
            if not os.path.isfile(self.outdir + ("/A.%d" % j)):
                self.execution.returncode = 1
                return
        self.execution.returncode = 0


class ApplicationWithOneOutputFile(Application):

        def terminated(self):
            if not os.path.isfile(self.outfile):
                self.execution.exitcode = 1


class B(ApplicationWithOneOutputFile):
    def __init__(self, infile, outfile, **kwargs):
        self.infile = infile
        self.outfile = outfile
        gc3libs.Application.__init__(
            self,
            arguments = [
                "./b2c.py",
                "-i", infile,
                "-o", outfile,
            ],
            inputs = ['b2c.py'],
            outputs = [],
            #join = True,
            stdout = "stdout.log",
            stderr = "stderr.log",
            **kwargs
        )


class C(ApplicationWithOneOutputFile):
    def __init__(self, infile, outfile, **kwargs):
        self.infile = infile
        self.outfile = outfile
        gc3libs.Application.__init__(
            self,
            arguments = [
                "./c2d.py",
                "-i", infile,
                "-o", outfile,
            ],
            inputs = ['c2d.py'],
            outputs = [],
            #join = True,
            stdout = "stdout.log",
            stderr = "stderr.log",
            **kwargs
        )


class D(ApplicationWithOneOutputFile):
    def __init__(self, input_dir, outfile, **kwargs):

        self.input_dir = input_dir
        self.outfile = outfile

        gc3libs.Application.__init__(
            self,
            arguments = [
                "./d.py", input_dir,
                "-o", outfile,
            ],
            inputs = ['d.py'],
            outputs = [],
            #join = True,
            stdout = "stdout.log",
            stderr = "stderr.log",
            **kwargs
        )


######################## Support Classes / Workflow elements #############################


class MainSequentialFlow(StopOnError, SequentialTaskCollection):
    def __init__(self, k, **kwargs):
        self.k = k

        gc3libs.log.info("\t Calling MainSequentialFlow.__init({})".format(k))
        SequentialTaskCollection.__init__(self, [
            A(k, GLOBAL_OUTPUT_DIR, **kwargs),
            MainParallelFlow(k ,GLOBAL_OUTPUT_DIR, **kwargs),
            D(GLOBAL_OUTPUT_DIR, GLOBAL_OUTPUT_DIR + '/D.txt', **kwargs),
        ], **kwargs)

    def terminated(self):
        gc3libs.log.info("\t MainSequentialFlow.terminated [%s]" % self.execution.returncode)


class MainParallelFlow(ParallelTaskCollection):
    @gc3libs.debug.trace
    def __init__(self, k, output_dir_of_a, **kwargs):
        self.k = k
        self.output_dir_of_a = output_dir_of_a

        # use our knowledge of what A does (violates DRY, though)
        self.tasks = [
            InnerSequentialFlow(j, output_dir_of_a, **kwargs)
            for j in range(k)
        ]
        ParallelTaskCollection.__init__(self, self.tasks, **kwargs)

    def terminated(self):
        gc3libs.log.info("\t\tMainParallelFlow.terminated")


class InnerSequentialFlow(StopOnError, SequentialTaskCollection):
    @gc3libs.debug.trace
    def __init__(self, j, output_dir_of_a, **kwargs):
        # XXX: hard-code paths!
        output_of_a = ("%s/A.%d" % (output_dir_of_a, j))
        output_of_b = ("%s/B.%d" % (GLOBAL_OUTPUT_DIR, j))
        output_of_c = ("%s/C.%d" % (GLOBAL_OUTPUT_DIR, j))

        initial_tasks = [
            B(output_of_a, output_of_b, **kwargs),
            C(output_of_b, output_of_c, **kwargs),
        ]
        SequentialTaskCollection.__init__(self, initial_tasks, **kwargs)

    def terminated(self):
        gc3libs.log.info("\t\t\t\tInnerSequentialFlow.terminated [%d]" % self.execution.returncode)


############################# Main Session Creator (?) ###################################


class TestWorkflow(SessionBasedScript):
    """
    Test pipeline
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.0.2',
            )

    def setup_options(self):
        self.add_param("-k", type=int, default=7, metavar="NUM", help="Number of parallel strands.")

    def new_tasks(self, kwargs):
        yield test_workflow.MainSequentialFlow(self.params.k, **kwargs)


# run script
if __name__ == '__main__':
    import test_workflow
    TestWorkflow().run()
