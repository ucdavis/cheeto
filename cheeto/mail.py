#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : mail.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 14.06.2023

import logging
from pathlib import Path
from typing import Generator, List, Optional, Tuple

from airium import Airium
import sh

from jinja2 import Environment, FileSystemLoader

from .templating import PKG_TEMPLATES


class Mailx:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
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
            cmd = Mailx.reply_to(cmd, reply_to)
        if subject is not None:
            cmd = Mailx.subject(cmd, subject)
        return cmd.bake('--', to, _in=message)


class Email:

    def __init__(self, **kwargs):
        self.airium_kwargs = kwargs

    def header(self) -> str:
        raise NotImplemented()

    def paragraphs(self) -> Generator[str, None, None]:
        raise NotImplemented()


class NewAccountReady(Email):

    def header(self) -> str:
        return 'Account Ready'

    def paragraphs(self,
                   sitename: str,
                   username: str,
                   site_fqdn: str,
                   slurm_account: str,
                   slurm_partitions: List[str],
                   storages: List[Tuple[str, str]]) -> Generator[str, None, None]:
        a = Airium(**self.airium_kwargs)

