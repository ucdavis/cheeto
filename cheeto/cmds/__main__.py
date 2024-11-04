#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import sys

from . import (commands,
               config,
               database,
               hippo,
               monitor,
               nocloud,
               puppet,
               slurm)


def main():
    return commands.run()


if __name__ == '__main__':
    sys.exit(main())
