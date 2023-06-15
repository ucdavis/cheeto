#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : mail.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 14.06.2023

import logging
from typing import Optional

import sh


class Mail:

    def __init__(self):
        logger = logging.getLogger(__name__)
        self.cmd = sh.Command('mailx').bake()

    @staticmethod
    def reply_to(cmd: sh.Command, address: str) -> sh.Command:
        return cmd.bake('-a', f'reply-to: {address}')

    @staticmethod
    def subject(cmd: sh.Command, text: str) -> sh.Command:
        return cmd.bake('-s', text)

    def send(self, to: str,
                   message: str,
                   reply_to: Optional[str] = None,
                   subject: Optional[str] = None) -> sh.Command:
        cmd = self.cmd
        if reply_to is not None:
            cmd = Mail.reply_to(cmd, reply_to)
        if subject is not None:
            cmd = Mail.subject(cmd, subject)
        return cmd.bake('--', to, _in=message)
