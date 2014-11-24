#! /usr/bin/env python

import argparse
import os
import random
import sys


if __name__ == '__main__':
    # parse command-line
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", type=str, help="Path to input file")
    parser.add_argument("-o", type=str, help="Path to output file")
    args = parser.parse_args()

    # fail with 33% probability
    if random.randint(0,3) == 0:
        sys.exit(3)

    infile = open(args.i, 'r')
    num = int(infile.read())

    # do the other half of the "half or triple plus one" thing
    if (num % 2) != 0:
        num = 3*num + 1

    outfile = open(args.o, 'w')
    outfile.write(str(num))
    outfile.close()
