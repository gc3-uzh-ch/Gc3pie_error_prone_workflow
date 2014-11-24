#! /usr/bin/env python

import argparse
import os
import random
import sys


if __name__ == '__main__':
    # parse command-line
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", type=int, help="Seed the RNG")
    parser.add_argument("-o", type=str, help="Stem of output files to create")
    parser.add_argument("-n", type=int, help="Number of output files to create")
    args = parser.parse_args()

    random.seed(args.i)

    for n in range(args.n):
        f = open((args.o + '.' + str(n)), 'w')
        f.write(str(random.randint(0, 100)))
        f.close()
