cmake_minimum_required(VERSION 3.17)
project(python_cdb C)

set(CMAKE_C_STANDARD 11)

include_directories(src)
include_directories(/usr/include/python3.8/)

add_executable(python_cdb
        src/cdb.c
        src/cdb.h
        src/cdb_hash.c
        src/cdb_make.c
        src/cdb_make.h
        src/cdbmodule.c
        src/uint32.h
        src/uint32_pack.c
        src/uint32_unpack.c src/unicode.c src/unicode.h)
