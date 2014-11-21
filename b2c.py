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

    # fail if `num` is divisible by 3
    if (num % 3) == 0:
        sys.exit(3)

    # else, write `num` unchanged
    outfile = open(args.o, 'w')
    outfile.write(str(num))
    outfile.close()
