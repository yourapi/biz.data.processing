from __future__ import absolute_import
import os, sys, time
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())

filename = sys.argv[1]
f = open(filename,'w')
for i in range(30000):
    f.write('{}\n'.format(i))
    time.sleep(.001)