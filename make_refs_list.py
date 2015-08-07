#! /usr/bin/env python

import os
import json


l = os.listdir('fm_indices')
j = json.dumps(l)
print(j)
