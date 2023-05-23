#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : conftest.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 23.05.2023

from distutils import dir_util
import os

import pytest


@pytest.fixture
def datadir(tmpdir, request):
    '''
    Fixture responsible for locating the test data directory and copying it
    into a temporary directory.
    '''
    filename = request.module.__file__
    test_dir = os.path.dirname(filename)
    data_dir = os.path.join(test_dir, 'test-data') 
    dir_util.copy_tree(data_dir, str(tmpdir))

    def getter(filename, as_str=True):
        filepath = tmpdir.join(filename)
        if as_str:
            return str(filepath)
        return filepath

    return getter


@pytest.fixture
def sacctmgr(tmpdir, datadir):

    pass
