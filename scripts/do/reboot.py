#! /usr/bin/env python2

import sys, do

def help():
  print """
Usage:
  reboot.py master
  reboot.py workers
"""

if __name__ == '__main__':
  if len(sys.argv) == 2:
    if 'workers' == sys.argv[1]:
      worker_img_id = do.get_image_id(do.WORKER_IMAGE) 
      do.reboot_by_image_id(worker_img_id)
    elif 'master' == sys.argv[1]:
      master_img_id = do.get_image_id(do.MASTER_IMAGE)
      do.reboot_by_image_id(master_img_id)
    else:
      help()
  else:
    help()
