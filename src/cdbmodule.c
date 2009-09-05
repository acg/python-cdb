/**
python-cdb 0.34
Copyright 2001-2009 Michael J. Pomraning <mjp@pilcrow.madison.wi.us>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

**/

#include <Python.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include "cdb.h"
#include "cdb_make.h"

#define open_read(x)       (open((x),O_RDONLY|O_NDELAY))
/* ala djb's open_foo */

#define VERSION     "0.34"
#define CDBVERSION  "0.75"

/* ------------------- cdb object -------------------- */

static char cdbo_object_doc[] = "\
This object represents a CDB database:  a reliable, constant\n\
database mapping strings of bytes (\"keys\") to strings of bytes\n\
(\"data\"), and designed for fast lookups.\n\
\n\
Unlike a conventional mapping, CDBs can meaningfully store multiple\n\
records under one key (though this feature is not often used).\n\
\n\
A CDB object 'cdb_o' offers the following interesting attributes:\n\
\n\
  Dict-like Lookup Methods:\n\
    cdb_o[key], get(key), getnext(), getall(key)\n\
\n\
  Key-based Iteration Methods:\n\
    keys(), firstkey(), nextkey()\n\
    (Key-based iteration returns only distinct keys.)\n\
\n\
  Raw Iteration Method:\n\
    each()\n\
    (\"Dumping\" may return the same key more than once.)\n\
\n\
  __members__:\n\
    fd   - File descriptor of the underlying cdb.\n\
    name - Name of the cdb, or None if not known.\n\
    size - Size of the cdb, or None if not mmap()d.\n\
\n\
  __length__:\n\
    len(cdb_o) returns the total number of items in a cdb,\n\
    which may or may not exceed the number of distinct keys.\n";
  

typedef struct {
    PyObject_HEAD
    struct cdb c;
    PyObject * name_py;  /* 'filename' or Py_None */
    PyObject * getkey;   /* squirreled away for getnext() */
    uint32 eod;          /* as in cdbdump */
    uint32 iter_pos;
    uint32 each_pos;
    uint32 numrecords;
} CdbObject;

staticforward PyTypeObject CdbType;

PyObject * CDBError;
#define CDBerr PyErr_SetFromErrno(CDBError)

static PyObject *
cdb_pyread(CdbObject *cdb_o, unsigned int len, uint32 pos) {
  struct cdb *c;
  PyObject *s = NULL;

  c = &cdb_o->c;

  if (c->map) {
    if ((pos > c->size) || (c->size - pos < len))
      goto FORMAT;
    s = PyString_FromStringAndSize(c->map + pos, len);
  } else {
    s = PyString_FromStringAndSize(NULL, len);
    if (s == NULL)
      return NULL;
    if (lseek(c->fd,pos,SEEK_SET) == -1) goto ERRNO;
    while (len > 0) {
      int r;
      char * buf = PyString_AsString(s);

      do {
        Py_BEGIN_ALLOW_THREADS
        r = read(c->fd,buf,len);
        Py_END_ALLOW_THREADS
      }
      while ((r == -1) && (errno == EINTR));
      if (r == -1) goto ERRNO;
      if (r == 0) goto FORMAT;
      buf += r;
      len -= r;
    }
  }

  return s;

  FORMAT:
  Py_XDECREF(s);
  PyErr_SetFromErrno(PyExc_RuntimeError);
  return NULL;

  ERRNO:
  Py_XDECREF(s);
  return CDBerr;

}


#define CDBO_CURDATA(x) (cdb_pyread(x, x->c.dlen, x->c.dpos))


/* ------------------- CdbObject methods -------------------- */

static char cdbo_has_key_doc[] =
"cdb_o.has_key(k) -> 1 (or 0)\n\
\n\
Returns true if the CDB contains key k.";

static PyObject *
cdbo_has_key(CdbObject *self, PyObject *args) {

  char * key;
  unsigned int klen;
  int r;

  if (!PyArg_ParseTuple(args, "s#", &key, &klen))
    return NULL;

  r = cdb_find(&self->c, key, klen);
  if (r == -1)
    return CDBerr;

  return Py_BuildValue("i", r);

}

static char cdbo_get_doc[] =
"cdb_o.get(k [, i]) -> data (or None)\n\
\n\
Fetches the record stored under key k, skipping past the first i\n\
records under that key (default: 0).  Prepares the next call to\n\
getnext().\n\
\n\
Assuming cdb_o.has_key(k) == 1, then all of the following return:\n\
the first record stored under key k:\n\
\n\
    cdb_o.get(k) == cdb_o[k] == cdb_o.getall(k)[0]\n";

static PyObject *
cdbo_get(CdbObject *self, PyObject *args) {

  char * key;
  unsigned int klen;
  int r;
  int i = 0;

  if (!PyArg_ParseTuple(args, "s#|i:get", &key, &klen, &i))
    return NULL;

  cdb_findstart(&self->c);

  for (;;) {
    r = cdb_findnext(&self->c, key, klen);
    if (r == -1) return CDBerr;
    if (!r) return Py_BuildValue("");
    if (!i) break;
    --i;
  }

  /* prep. possibly ensuing call to getnext() */
  Py_XDECREF(self->getkey);
  self->getkey = PyString_FromStringAndSize(key, klen);
  if (self->getkey == NULL)
    return NULL;

  return CDBO_CURDATA(self);
}

static char cdbo_getall_doc[] =
"cdb_o.getall(k) -> ['data', ... ]\n\
\n\
Return a list of all records stored under key k.";

static PyObject *
cdbo_getall(CdbObject *self, PyObject *args) {

  PyObject * list, * data;
  char * key;
  unsigned int klen;
  int r, err;

  if (!PyArg_ParseTuple(args, "s#:getall", &key, &klen))
    return NULL;

  list = PyList_New(0);

  if (list == NULL) return NULL;

  cdb_findstart(&self->c);

  while ((r = cdb_findnext(&self->c, key, klen))) {
    if (r == -1) {
      Py_DECREF(list);
      return CDBerr;
    }
    data = CDBO_CURDATA(self);
    if (data == NULL) {
      Py_DECREF(list);
      return NULL;
    }
    err = PyList_Append(list, data);
    Py_DECREF(data);
    if (err != 0) {
      Py_DECREF(list);
      return NULL;
    }
  }

  return list;

}

static char cdbo_getnext_doc[] =
"cdb_o.getnext() -> 'data' (or None)\n\
\n\
For iteration over the records stored under one key, avoiding loading\n\
all items into memory).  The \"current key\" is determined by the most\n\
recent call to get().\n\
\n\
The following loops through all items stored under key k:\n\
\n\
    ## cdb_o.getall(k) possibly too big for memory\n\
    rec = cdb_o.get(k)\n\
    while rec is not None:\n\
      do_something(rec)\n\
      rec = cdb_o.getnext()\n";

static PyObject *
cdbo_getnext(CdbObject *self, PyObject *args) {

  if (!PyArg_ParseTuple(args, ":getnext"))
    return NULL;

  if (self->getkey == NULL) {
    PyErr_SetString(PyExc_TypeError, 
                    "getnext() called without first calling get()");
    return NULL;
  }

  switch(cdb_findnext(&self->c, 
                      PyString_AsString(self->getkey), 
                      PyString_Size(self->getkey))) {
    case -1:
      return CDBerr;
    case  0:
      Py_DECREF(self->getkey);
      self->getkey = NULL;
      return Py_BuildValue("");
    default:
      return CDBO_CURDATA(self);
  }
 /* not reached */
}

uint32
_cdbo_init_eod(CdbObject *self) {

  char nonce[4];

  if (cdb_read(&self->c, nonce, 4, 0) == -1)
    return 0;

  uint32_unpack(nonce, &self->eod);

  return self->eod;

}

/*
 * _cdbo_keyiter(cdb_o)
 *
 * Whiz-bang all-in-one:
 *   extract current record
 *   compare current pos to pos implied by cdb_find(current_key)
 *     (Different? adv. iter cursor, loop and try again)
 *   advance iteration cursor
 *   return key
 */

static PyObject *
_cdbo_keyiter(CdbObject *self) {

  PyObject *key;
  char buf[8];
  uint32 klen, dlen;

  if (! self->eod)
    _cdbo_init_eod(self);

  while (self->iter_pos < self->eod) {
    if (cdb_read(&self->c, buf, 8, self->iter_pos) == -1)
      return CDBerr;

    uint32_unpack(buf, &klen);
    uint32_unpack(buf+4, &dlen);

    key = cdb_pyread(self, klen, self->iter_pos + 8);

    if (key == NULL)
      return NULL;

    switch(cdb_find(&self->c,PyString_AsString(key),PyString_Size(key))) {
      case -1:
        Py_DECREF(key);
        key = NULL;
        return CDBerr;
      case 0:
        /* bizarre, impossible? PyExc_RuntimeError? */
        PyErr_SetString(PyExc_KeyError, 
                        PyString_AS_STRING((PyStringObject *) key));
        Py_DECREF(key);
        key = NULL;
      default:
        if (key == NULL)  /* already raised error */
          return NULL;

        if (cdb_datapos(&self->c) == self->iter_pos + klen + 8) {
          /** first occurrence of key in the cdb **/
          self->iter_pos += 8 + klen + dlen;
          return key;
        }
        Py_DECREF(key);   /* better luck next time around */
        self->iter_pos += 8 + klen + dlen;
    }
  }

  return Py_BuildValue("");  /* iter_pos >= eod; we're done */

}

static char cdbo_keys_doc[] =
"cdb_o.keys() -> ['k1', 'k2', ... ]\n\
\n\
Returns a list of all (distinct) keys in the database.";

static PyObject *
cdbo_keys(CdbObject *self, PyObject *args) {

  PyObject *r, *key;
  uint32 pos;
  int err;

  if (! PyArg_ParseTuple(args, ""))
    return NULL;

  r = PyList_New(0);
  if (r == NULL)
    return NULL;

  pos = self->iter_pos;  /* don't interrupt a manual iteration */

  self->iter_pos = 2048;

  key = _cdbo_keyiter(self);
  while (key != Py_None) {
    err = PyList_Append(r, key);
    Py_DECREF(key);
    if (err != 0) {
      Py_DECREF(r);
      self->iter_pos = pos;
      return NULL;
    }
    key = _cdbo_keyiter(self);
  }
  Py_DECREF(key);

  self->iter_pos = pos;

  return r;

}

static char cdbo_firstkey_doc[] =
"cdb_o.firstkey() -> key (or None)\n\
\n\
Return the first key in the database, resetting the internal\n\
iteration cursor.  firstkey() and nextkey() may be used to\n\
traverse all distinct keys in the cdb. See each() for raw\n\
iteration.";

static PyObject *
cdbo_firstkey(CdbObject *self, PyObject *args) {

  if (! PyArg_ParseTuple(args, ":firstkey"))
    return NULL;

  self->iter_pos = 2048;

  return _cdbo_keyiter(self);

}

static char cdbo_nextkey_doc[] =
"cdb_o.nextkey() -> key (or None)\n\
\n\
Return the next distinct key in the cdb.\n\
\n\
The following code walks the CDB one key at a time:\n\
\n\
    key = cdb_o.firstkey()\n\
    while key is not None:\n\
        print key\n\
        key = cdb_o.nextkey()\n";

static PyObject *
cdbo_nextkey(CdbObject *self, PyObject *args) {

  if (! PyArg_ParseTuple(args, ":nextkey"))
    return NULL;

  return _cdbo_keyiter(self);

}

static char cdbo_each_doc[] =
"cdb_o.each() -> (key, data) (or None)\n\
\n\
Fetch the next ('key', 'data') record from the underlying cdb file,\n\
returning None and resetting the iteration cursor when all records\n\
have been fetched.\n\
\n\
Keys appear with each item under them -- e.g., (key,foo), (key2,bar),\n\
(key,baz) --  order of records is determined by actual position on\n\
disk.  Both keys() and (for GDBM fanciers) firstkey()/nextkey()-style\n\
iteration go to pains to present the user with only distinct keys.";

static PyObject *
cdbo_each(CdbObject *self, PyObject *args) {

  PyObject *tup, *key, *dat;
  char buf[8];
  uint32 klen, dlen;

  if (! PyArg_ParseTuple(args, ":each"))
    return NULL;

  tup = PyTuple_New(2);
  if (tup == NULL)
    return NULL;

  if (! self->eod)
    (void) _cdbo_init_eod(self);

  if (self->each_pos >= self->eod) { /* all done, reset cursor */
    self->each_pos = 2048;
    Py_INCREF(Py_None);
    return Py_None;
  }

  if (cdb_read(&self->c, buf, 8, self->each_pos) == -1)
    return CDBerr;

  uint32_unpack(buf, &klen);
  uint32_unpack(buf+4, &dlen);

  key = cdb_pyread(self, klen, self->each_pos + 8);
  dat = cdb_pyread(self, dlen, self->each_pos + 8 + klen);

  self->each_pos += klen + dlen + 8;

  if (key == NULL || dat == NULL) {
    Py_XDECREF(key); Py_XDECREF(dat);
    Py_DECREF(tup);
    return NULL;
  }

  if (PyTuple_SetItem(tup, 0, key) || PyTuple_SetItem(tup, 1, dat)) {
    Py_DECREF(key); Py_DECREF(dat); Py_DECREF(tup);
    return NULL;
  }

  return tup;
}

/*** cdb object as mapping ***/

static int
cdbo_length(CdbObject *self) {

  if (! self->numrecords) {
    char buf[8];
    uint32 pos, klen, dlen;

    pos = 2048;

    if (! self->eod)
      (void) _cdbo_init_eod(self);

    while (pos < self->eod) {
      if (cdb_read(&self->c, buf, 8, pos) == -1)
        return -1;
      uint32_unpack(buf, &klen);
      uint32_unpack(buf + 4, &dlen);
      pos += 8 + klen + dlen;
      self->numrecords++;
    }
  }
  return (int) self->numrecords;
}

static PyObject *
cdbo_subscript(CdbObject *self, PyObject *k) {
  char * key;
  int klen;

  if (! PyArg_Parse(k, "s#", &key, &klen))
    return NULL;

  switch(cdb_find(&self->c, key, (unsigned int)klen)) {
    case -1:
      return CDBerr;
    case 0:
      PyErr_SetString(PyExc_KeyError, 
                      PyString_AS_STRING((PyStringObject *) k));
      return NULL;
    default:
      return CDBO_CURDATA(self);
  }
  /* not reached */
}

static PyMappingMethods cdbo_as_mapping = {
	(inquiry)cdbo_length,
	(binaryfunc)cdbo_subscript,
	(objobjargproc)0
};

static PyMethodDef cdb_methods[] = { 

  {"get",      (PyCFunction)cdbo_get,      METH_VARARGS,
               cdbo_get_doc },
  {"getnext",  (PyCFunction)cdbo_getnext,  METH_VARARGS,
               cdbo_getnext_doc },
  {"getall",   (PyCFunction)cdbo_getall,   METH_VARARGS,
               cdbo_getall_doc },
  {"has_key",  (PyCFunction)cdbo_has_key,  METH_VARARGS, 
               cdbo_has_key_doc },
  {"keys",     (PyCFunction)cdbo_keys,     METH_VARARGS,
               cdbo_keys_doc },
  {"firstkey", (PyCFunction)cdbo_firstkey, METH_VARARGS,
               cdbo_firstkey_doc },
  {"nextkey",  (PyCFunction)cdbo_nextkey,  METH_VARARGS,
               cdbo_nextkey_doc },
  {"each",     (PyCFunction)cdbo_each, METH_VARARGS,
               cdbo_each_doc },
  { NULL,    NULL }
};

/* ------------------- cdb operations -------------------- */

static PyObject *
_wrap_cdb_init(int fd) {  /* constructor implementation */

  CdbObject *self;

  self = PyObject_NEW(CdbObject, &CdbType);
  if (self == NULL) return NULL;

  self->c.map = 0; /* break encapsulation -- cdb struct init'd to zero */
  cdb_init(&self->c, fd);

  self->iter_pos   = 2048;
  self->each_pos   = 2048;
  self->numrecords = 0;
  self->eod        = 0;
  self->getkey     = NULL;

  return (PyObject *) self;
}


static PyObject *
cdbo_constructor(PyObject *ignore, PyObject *args) {

  PyObject *self;
  PyObject *f;
  PyObject *name_attr = Py_None;
  int fd;

  if (! PyArg_ParseTuple(args, "O:new", &f))
    return NULL;

  if (PyString_Check(f)) {

    if ((fd = open_read(PyString_AsString(f))) == -1)
      return CDBerr;

    name_attr = f;

  } else if (PyInt_Check(f)) {

    fd = (int) PyInt_AsLong(f);

  } else {

    PyErr_SetString(PyExc_TypeError,
                    "expected filename or file descriptor");
    return NULL;

  }

  self = _wrap_cdb_init(fd);
  if (self == NULL) return NULL;

  ((CdbObject *)self)->name_py = name_attr;
  Py_INCREF(name_attr);

  return self;
}

static void
cdbo_dealloc(CdbObject *self) {  /* del(cdb_o) */

  if (self->name_py != NULL) {

    /* if cdb_o.name is not None:  we open()d it ourselves, so close it */
    if (PyString_Check(self->name_py))
      close(self->c.fd);

    Py_DECREF(self->name_py);
  }

  Py_XDECREF(self->getkey);

  cdb_free(&self->c);

  PyObject_DEL(self);
}

static PyObject *
cdbo_getattr(CdbObject *self, char *name) {

  PyObject * r;

  r = Py_FindMethod(cdb_methods, (PyObject *) self, name);

  if (r != NULL)
    return r;

  PyErr_Clear();

  if (!strcmp(name,"__members__"))
    return Py_BuildValue("[sss]", "fd", "name", "size");

  if (!strcmp(name,"fd")) {
    return Py_BuildValue("i", self->c.fd);  /* cdb_o.fd */
  } 

  if (!strcmp(name,"name")) {
    Py_INCREF(self->name_py);
    return self->name_py;                   /* cdb_o.name */
  } 

  if (!strcmp(name,"size")) {               /* cdb_o.size */
    return self->c.map ?  /** mmap()d ? stat.st_size : None **/
           Py_BuildValue("l", (long) self->c.size) :
           Py_BuildValue("");
  } 

  PyErr_SetString(PyExc_AttributeError, name);
  return NULL;
}


/* ----------------- cdbmake object ------------------ */

static char cdbmake_object_doc[] =
"cdbmake objects resemble the struct cdb_make interface:\n\
\n\
  CDB Construction Methods:\n\
    add(k, v), finish()\n\
\n\
  __members__:\n\
    fd         - fd of underlying CDB, or -1 if finish()ed\n\
    fn, fntmp  - as from the cdb package's cdbmake utility\n\
    numentries - current number of records add()ed\n";

typedef struct {
    PyObject_HEAD
    struct cdb_make cm;
    PyObject * fn;
    PyObject * fntmp;
} cdbmakeobject;

staticforward PyTypeObject CdbMakeType;

#define CDBMAKEerr PyErr_SetFromErrno(PyExc_IOError)


/* ----------------- CdbMake methods ------------------ */

static PyObject *
CdbMake_add(cdbmakeobject *self, PyObject *args) {

  char * key, * dat;
  unsigned int klen, dlen;

  if (!PyArg_ParseTuple(args,"s#s#:add",&key,&klen,&dat,&dlen))
    return NULL;

  if (cdb_make_add(&self->cm, key, klen, dat, dlen) == -1)
    return CDBMAKEerr;

  return Py_BuildValue("");

}

static PyObject *
CdbMake_finish(cdbmakeobject *self, PyObject *args) {

  if (!PyArg_ParseTuple(args, ":finish"))
    return NULL;

  if (cdb_make_finish(&self->cm) == -1)
    return CDBMAKEerr;

  /* cleanup as in cdb dist's cdbmake */

  if (fsync(fileno(self->cm.fp)) == -1)
    return CDBMAKEerr;

  if (fclose(self->cm.fp) != 0)
    return CDBMAKEerr;

  self->cm.fp = NULL;

  if (rename(PyString_AsString(self->fntmp),
             PyString_AsString(self->fn))    == -1)
    return CDBMAKEerr;

  return Py_BuildValue("");
}

static PyMethodDef cdbmake_methods[] = {
  {"add",    (PyCFunction)CdbMake_add,    METH_VARARGS,
"cm.add(key, data) -> None\n\
\n\
Add 'key' -> 'data' pair to the underlying CDB." },
  {"finish", (PyCFunction)CdbMake_finish, METH_VARARGS,
"cm.finish() -> None\n\
\n\
Finish safely composing a new CDB, renaming cm.fntmp to\n\
cm.fn." },
  { NULL,    NULL }
};

/* ----------------- cdbmake operations ------------------ */

static PyObject *
new_cdbmake(PyObject *ignore, PyObject *args) {

  cdbmakeobject *self;
  PyObject *fn, *fntmp;
  FILE * f;

  if (! PyArg_ParseTuple(args, "SS|i", &fn, &fntmp))
    return NULL;

  f = fopen(PyString_AsString(fntmp), "w+b");
  if (f == NULL) {
    return CDBMAKEerr;
  }

  self = PyObject_NEW(cdbmakeobject, &CdbMakeType);
  if (self == NULL) return NULL;

  self->fn = fn;
  Py_INCREF(self->fn);

  self->fntmp = fntmp;
  Py_INCREF(fntmp);

  if (cdb_make_start(&self->cm, f) == -1) {
    Py_DECREF(self);
    CDBMAKEerr;
    return NULL;
  }

  return (PyObject *) self;
}

static void
cdbmake_dealloc(cdbmakeobject *self) {

  Py_XDECREF(self->fn);

  if (self->fntmp != NULL) {
    if (self->cm.fp != NULL) {
      fclose(self->cm.fp);
      unlink(PyString_AsString(self->fntmp));
    }
    Py_DECREF(self->fntmp);
  }

  PyObject_DEL(self);
}

static PyObject *
cdbmake_getattr(cdbmakeobject *self, char *name) {

  if (!strcmp(name,"__members__"))
    return Py_BuildValue("[ssss]", "fd", "fn", "fntmp", "numentries");

  if (!strcmp(name,"fd"))
    return Py_BuildValue("i", fileno(self->cm.fp));  /* self.fd */

  if (!strcmp(name,"fn")) {
    Py_INCREF(self->fn);
    return self->fn;                         /* self.fn */
  }

  if (!strcmp(name,"fntmp")) {
    Py_INCREF(self->fntmp);
    return self->fntmp;                      /* self.fntmp */
  }

  if (!strcmp(name,"numentries"))
    return Py_BuildValue("l", self->cm.numentries); /* self.numentries */

  return Py_FindMethod(cdbmake_methods, (PyObject *) self, name);
}

/* ---------------- Type delineation -------------------- */

statichere PyTypeObject CdbType = {
        /* The ob_type field must be initialized in the module init function
         * to be portable to Windows without using C++. */
        PyObject_HEAD_INIT(NULL)
        0,                      /*ob_size*/
        "cdb",              /*tp_name*/
        sizeof(CdbObject),  /*tp_basicsize*/
        0,                      /*tp_itemsize*/
        /* methods */
        (destructor)cdbo_dealloc, /*tp_dealloc*/
        0,                      /*tp_print*/
        (getattrfunc)cdbo_getattr, /*tp_getattr*/
        0,                      /*tp_setattr*/
        0,                      /*tp_compare*/
        0,                      /*tp_repr*/
        0,                      /*tp_as_number*/
        0,                      /*tp_as_sequence*/
        &cdbo_as_mapping,       /*tp_as_mapping*/
        0,                      /*tp_hash*/
        0,                      /*tp_call*/
        0,                      /*tp_str*/
        0,                      /*tp_getattro*/
        0,                      /*tp_setattro*/
        0,                      /*tp_as_buffer*/
        0,                      /*tp_xxx4*/
        cdbo_object_doc,        /*tp_doc*/
};

statichere PyTypeObject CdbMakeType = {
        /* The ob_type field must be initialized in the module init function
         * to be portable to Windows without using C++. */
        PyObject_HEAD_INIT(NULL)
        0,                      /*ob_size*/
        "cdbmake",              /*tp_name*/
        sizeof(cdbmakeobject),  /*tp_basicsize*/
        0,                      /*tp_itemsize*/
        /* methods */
        (destructor)cdbmake_dealloc, /*tp_dealloc*/
        0,                      /*tp_print*/
        (getattrfunc)cdbmake_getattr, /*tp_getattr*/
        0,                      /*tp_setattr*/
        0,                      /*tp_compare*/
        0,                      /*tp_repr*/
        0,                      /*tp_as_number*/
        0,                      /*tp_as_sequence*/
        0,                      /*tp_as_mapping*/
        0,                      /*tp_hash*/
        0,                      /*tp_call*/
        0,                      /*tp_str*/
        0,                      /*tp_getattro*/
        0,                      /*tp_setattro*/
        0,                      /*tp_as_buffer*/
        0,                      /*tp_xxx4*/
        cdbmake_object_doc,     /*tp_doc*/
};

/* ---------------- exported functions ------------------ */
static PyObject *
_wrap_cdb_hash(PyObject *ignore, PyObject *args) {

  char *s;
  int sz;

  if (! PyArg_ParseTuple(args, "s#:hash", &s, &sz))
    return NULL;

  return Py_BuildValue("l", cdb_hash(s, (unsigned int) sz));

}

/* ---------------- cdb Module -------------------- */

static PyMethodDef module_functions[] = {
  {"init",    cdbo_constructor, METH_VARARGS,
"cdb.init(f) -> cdb_object\n\
\n\
Open a CDB specified by f and return a cdb object.\n\
f may be a filename or an integral file descriptor\n\
(e.g., init( sys.stdin.fileno() )...)."},
  {"cdbmake", new_cdbmake,     METH_VARARGS,
"cdb.cdbmake(cdb, tmp) -> cdbmake_object\n\
\n\
Interface to the creation of a new CDB file \"cdb\".\n\
\n\
The cdbmake object first writes records to the temporary file\n\
\"tmp\" (records are inserted via the object's add() method).\n\
The finish() method then atomically renames \"tmp\" to \"cdb\",\n\
ensuring that readers of \"cdb\" need never wait for updates to\n\
complete."
},
  {"hash",    _wrap_cdb_hash,  METH_VARARGS,
"hash(s) -> hashval\n\
\n\
Compute the 32-bit hash value of some sequence of bytes s."},
  {NULL,  NULL}
};

static char module_doc[] =
"Python adaptation of D. J. Bernstein's constant database (CDB)\n\
package.  See <http://cr.yp.to/cdb.html>\n\
\n\
CDB objects, created by init(), provide read-only, dict-like\n\
access to cdb files, as well as iterative methods.\n\
\n\
CDBMake objects, created by cdbmake(), allow for creation and\n\
atomic replacement of CDBs.\n\
\n\
This module defines a new Exception \"error\".";

DL_EXPORT(void)
initcdb() {
  PyObject *m, *d, *v;

  CdbType.ob_type = &PyType_Type;
  CdbMakeType.ob_type = &PyType_Type;

  m = Py_InitModule3("cdb", module_functions, module_doc);

  d = PyModule_GetDict(m);

  CDBError = PyErr_NewException("cdb.error", NULL, NULL);
  PyDict_SetItemString(d, "error", CDBError);

  PyDict_SetItemString(d, "__version__", 
                       v = PyString_FromString(VERSION));
  PyDict_SetItemString(d, "__cdb_version__",
                       v = PyString_FromString(CDBVERSION));
  Py_XDECREF(v);

}
