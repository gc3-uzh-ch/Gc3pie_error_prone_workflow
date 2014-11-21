#! /usr/bin/env python

import argparse
import os
import random
import sys


if __name__ == '__main__':
    # parse command-line
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", dest="input",  type=int, help="Seed the RNG")
    parser.add_argument("-o", dest="output", type=str, help="Stem of output files to create")
    parser.add_argument("-n", dest="num",    type=int, help="Number of output files to create")
    args = parser.parse_args()

    random.seed(args.input)

    for n in range(args.n):
        f = open((args.output + '.' + str(n)), 'w')
        f.write(random.randint(0, 100))
        f.close()
