import digitalocean

DEFAULT_SIZE_ID = 66 # the smallest node size = 512Mb
        # 66 the smallest node size = 512Mb
        # 2Gb/2CPU server has id=62
        # 63  1GB
        # 62  2GB
        # 64  4GB
REGION_ID = 8 # New York 3 datacenter (nyc3)

WORKER_IMAGE = "WORKER_BWT"
MASTER_IMAGE = "MASTER_BWT"

clientID = "7CTB3uVjkRTXLNtXj6TOU"
apiKey = "MHXhgeWFSFXjxVVakf2Ip5euiV1ZJlg55MFhXDkG5"

def destroy():
  manager = get_manager()
  nodes = manager.get_all_droplets()
  master_img_id = get_image_id(MASTER_IMAGE)
  worker_img_id = get_image_id(WORKER_IMAGE)
  terminator_counter = 0
  for n in nodes:
    if master_img_id == n.image_id or worker_img_id == n.image_id:
      n.destroy()
      terminator_counter += 1
  print "Destroyed %i droplets" % terminator_counter

def create_master(size_id):
  d = digitalocean.Droplet()
  d.client_id = clientID
  d.api_key = apiKey
  d.name = 'master'
  d.size_id = size_id
  d.region_id = REGION_ID 
  d.image_id = get_image_id(MASTER_IMAGE)
  d.create()

def create_worker(size_id, number):
  for n in xrange(number):  
    d = digitalocean.Droplet()
    d.client_id = clientID
    d.api_key = apiKey
    d.name = 'worker%i' % n
    d.size_id = size_id
    d.region_id = REGION_ID 
    d.image_id = get_image_id(WORKER_IMAGE)
    d.create()

def get_image_id(image_name):
    """
    returns image_id of image having the given image_name
    """
    manager = digitalocean.Manager(client_id=clientID, api_key=apiKey)
    my_images = manager.get_my_images()
    for img in my_images:
        if img.name == image_name:
            return img.id
    else:
        return None

def get_manager():
    return digitalocean.Manager(client_id=clientID, api_key=apiKey)

def reboot_by_image_id(image_id):
  manager = do.get_manager()
  droplets = manager.get_all_droplets()
  for d in droplets:
    if d.image_id == image_id:
      d.reboot()
