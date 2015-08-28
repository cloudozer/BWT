#! /usr/bin/env python

import os
import json

PATH = 'fm_indices'
files = os.listdir(PATH)
data = [[f, os.stat(os.path.join(PATH, f)).st_size] for f in files]
j = json.dumps(data)
print(j)
