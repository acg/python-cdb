//
// Created by desya on 3/1/21.
//

#include <stdlib.h>
#include "unicode.h"

size_t pyunicode_to_multibyte_string(PyObject* pyunicode, char** ppbuf) {
    char* mb_string = NULL;
    wchar_t* unicode_str = PyUnicode_AsWideCharString(pyunicode, 0);
    size_t mb_string_sz = wcstombs(0, unicode_str, wcslen(unicode_str) * MB_CUR_MAX + 1);
    if (mb_string_sz < 0) {
        return mb_string_sz;
    }
    mb_string = malloc(mb_string_sz + 1);
    mb_string_sz = wcstombs(mb_string, unicode_str, wcslen(unicode_str) * MB_CUR_MAX + 1);
    mb_string[mb_string_sz] = 0;
    *ppbuf = mb_string;
    return mb_string_sz;
}