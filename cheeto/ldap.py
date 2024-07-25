#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 19.07.2024

from typing import List, Optional

import ldap


class LDAP:

    def __init__(self, servers: List[str], 
                       login_dn: Optional[str],
                       password: Optional[str]):
        pass
