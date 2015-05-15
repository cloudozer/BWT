#! /usr/bin/env python2

import sys, do

if __name__ == '__main__':
  if len(sys.argv) == 2 and sys.argv[1] == 'master':
    do.create_master(do.DEFAULT_SIZE_ID) 
    print "Done"
  elif len(sys.argv) == 3 and sys.argv[1] == 'worker':
    number = int(sys.argv[2])
    do.create_worker(do.DEFAULT_SIZE_ID, number)
    print "Done"
  else:
    print """
Usage:
  ./setup.py master
  ./setup.py worker %number%
"""
