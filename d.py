#! /usr/bin/env python

import argparse
import os
import sys


if __name__ == '__main__':
    # parse command-line
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", type=str, help="Path to input file")
    args = parser.parse_args()

    infile = open(args.i, 'r')

    ones = 0
    for line in infile:
        num = int(line)
        if num == 1:
            ones += 1

    print("Number of ones seen: %d" % ones)
