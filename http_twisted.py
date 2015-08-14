#! /usr/bin/env python

from twisted.web.server import Site
from twisted.web.static import File
from twisted.internet import reactor
import sys

try:
  port = int(sys.argv[1])
except Exception as e:
  port = 8888
print('Serving HTTP on 0.0.0.0 port %i' % port)
reactor.listenTCP(port, Site(File("."))); reactor.run()
