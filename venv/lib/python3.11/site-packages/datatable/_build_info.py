#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# --------------------------------------------------------------
# Copyright 2018-2023 H2O.ai
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the 'Software'), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the
# Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY
# KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
# OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# --------------------------------------------------------------
# This file was auto-generated from ci/ext.py

import types

try:
    import datatable.lib._datatable as _dt
    _compiler = _dt._compiler()
except:
    _compiler = 'unknown'

build_info = types.SimpleNamespace(
    version='1.1.0',
    build_date='2023-11-29 23:58:47',
    build_mode='release',
    compiler=_compiler,
    git_revision='8d6ba1c66185cea5e0e8568dd5065e177067993b',
    git_branch='rel-1.1',
    git_date='2023-11-29 23:58:19',
    git_diff='',
)
