#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

from mongoengine import (DictField,
                         IntField,
                         StringField)

from ..types import HIPPO_EVENT_ACTIONS, HIPPO_EVENT_STATUSES

from .base import BaseDocument


class HippoEvent(BaseDocument):
    hippo_id = IntField(required=True, unique=True)
    action = StringField(required=True,
                         choices=HIPPO_EVENT_ACTIONS)
    n_tries = IntField(required=True, default=0)
    status = StringField(required=True,
                         default='Pending',
                         choices=HIPPO_EVENT_STATUSES)
    data = DictField()