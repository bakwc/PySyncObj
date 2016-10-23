#!/usr/bin/env python

from utility import Utility
import sys, os

def main(args=None):
    if args is None:
        args = sys.argv[1:]
    o = Utility(args)
    sys.stdout.write(o.getResult())
    sys.stdout.write(os.linesep)


if __name__ == '__main__':
    main()
