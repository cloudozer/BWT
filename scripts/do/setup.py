#! /usr/bin/env python

import sys, do

def str_to_size_id(mem_size_str):
  if '1G' == mem_size_str:
    return 63
  elif '2G' == mem_size_str:
    return 62
  elif '4G' == mem_size_str: 
    return 64
  else:
    raise Exception('no way') 

if __name__ == '__main__':
  if len(sys.argv) >= 2 and sys.argv[1] == 'master':
    try:
      size_id = str_to_size_id(sys.argv[2])
    except:
      print "Unknown size_id, use default value"
      size_id = do.DEFAULT_SIZE_ID
    do.create_master(size_id) 
    print "Done"
  elif len(sys.argv) >= 3 and sys.argv[1] == 'worker':
    number = int(sys.argv[2])
    try:
      size_id = str_to_size_id(sys.argv[3])
    except:
      print "Unknown size_id, use default value"
      size_id = do.DEFAULT_SIZE_ID
    do.create_worker(size_id, number)
    print "Done"
  else:
    print """
Usage:
  setup.py master [1G|2G|4G]
  setup.py worker %number% [1G|2G|4G]
"""
