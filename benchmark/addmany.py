#!/usr/bin/env python

import cdb
import sys
import time

cdbfile = sys.argv[1]
cdbtemp = "%s.tmp" % sys.argv[1]
count = 1000000
pairs = [ (str(s),str(s)) for s in xrange(count) ]

c = cdb.cdbmake( cdbfile, cdbtemp )
t0 = time.time()
for p in pairs: c.add(*p)
t1 = time.time()
c.finish()
print "cdbmake.add x %d = %f secs" % (count,t1-t0)

c = cdb.cdbmake( cdbfile, cdbtemp )
t0 = time.time()
c.addmany(pairs)
t1 = time.time()
c.finish()
print "cdbmake.addmany x %d = %f secs" % (count,t1-t0)


