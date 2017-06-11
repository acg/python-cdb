## INTRO

The `python-cdb` extension module is an adaptation of D. J. Bernstein's [constant database package](http://cr.yp.to/cdb.html).

`cdb` files are mappings of keys to values, designed for wickedly fast lookups and atomic updates.  This module mimics the normal `cdb` utilities, `cdb(get|dump|make)`, via convenient, high-level Python objects.


## INSTALL

```sh
tar zxf python-cdb-$VERSION.tgz
cd python-cdb-$VERSION
python setup.py build
python setup.py install
# python setup.py bdist --format=rpm, if you prefer
```

Now break it and tell me about it (or use it smoothly and tell me about that, too).


## DOCS

Consult the docstrings for module, class, and function documentation.

```sh
python -c 'import cdb; print cdb.__doc__'
python -c 'import cdb; print cdb.cdbmake("f.cdb","f.tmp").__doc__'
python -c 'import cdb; print cdb.init("some.cdb").__doc__'
```


## BUGS

Please report new bugs via the [Github issue tracker](https://github.com/acg/python-cdb/issues).


## TODO

- [ ] more dict-like API
- [ ] test cases
- [ ] take advantage of contemporary Python API
- [ ] formal speed benchmarks
- [ ] possibly revert to DJB's cdb implementation; explicitly public domain since 2007Q4
- [ ] better README/docs
- [ ] mingw support


## COPYRIGHT

`python-cdb` is free software, as is cdb itself.

The extension module is licensed under the GNU GPL version 2 or later, and is copyright 2001, 2002 Michael J. Pomraning.  Ancillary files from Felix von Leitner's libowfat are also licensed under the GPL.  Finally, modifications to D. J. Bernstein's public domain cdb implementation are similarly released to the public domain.


## AUTHORS

- Alan Grow <alangrow+python-cdb@gmail.com>
- Mike Pomraning <mjp@pilcrow.madison.wi.us>

