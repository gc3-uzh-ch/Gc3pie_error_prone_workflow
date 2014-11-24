#! /usr/bin/env python

import argparse
import os
import sys


if __name__ == '__main__':
    # parse command-line
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs='+', type=str, help="Path to input files")
    parser.add_argument("-o", type=str, help="Path to aggregate output file")
    args = parser.parse_args()

    output = open(args.o, 'w')

    ones = 0
    for infile in infile:
        num = int(line)
        if num == 1:
            ones += 1
        output.write("%s\n" % num)

    output.close()
    print("Number of ones seen: %d" % ones)
