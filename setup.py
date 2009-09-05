#! /usr/bin/env python

SRCDIR   = "src"
SRCFILES = map(lambda f: SRCDIR + '/' + f + '.c',
              ["cdbmodule","cdb","cdb_make","cdb_hash",
               "uint32_pack","uint32_unpack"])

from distutils.core import setup, Extension

setup (# Distribution meta-data
        name = "python-cdb",
        version = "0.34",
        description = "Interface to constant database files",
        author = "Mike Pomraning",
        author_email = "mjp@pilcrow.madison.wi.us",
	license = "GPL",
        long_description = \
'''The python-cdb extension module is an adaptation of D. J. Bernstein's
constant database package (see http://cr.yp.to/cdb.html).

cdb files are mappings of keys to values, designed for wickedly
fast lookups and atomic updates.  This module mimics the normal
cdb utilities, cdb(get|dump|make), via convenient, high-level Python
objects.''',
        ext_modules = [ Extension(
                            "cdbmodule",
                            SRCFILES,
                            include_dirs=[ SRCDIR + '/' ]
                        ) 
                      ],
        url = "http://pilcrow.madison.wi.us/",
      )

