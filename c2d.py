#! /usr/bin/env python

import argparse
import os
import sys


if __name__ == '__main__':
    # parse command-line
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", type=str, help="Path to input file")
    parser.add_argument("-o", type=str, help="Path to output file")
    args = parser.parse_args()

    infile = open(args.i, 'r')
    num = int(infile.read())

    # do the "half or triple plus one" thing
    if (num % 2) == 0:
        num /= 2
    else:
        num = 3*num + 1

    # XXX: race condition!!!
    outfile = open(args.o, 'a')
    outfile.write(str(num) + '\n')
    outfile.close()
