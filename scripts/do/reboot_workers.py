#! /usr/bin/env python2

import do

if __name__ == '__main__':
  worker_img_id = do.get_image_id(do.WORKER_IMAGE) 
  manager = do.get_manager()
  droplets = manager.get_all_droplets()
  for d in droplets:
    if d.image_id == worker_img_id:
      d.reboot()
