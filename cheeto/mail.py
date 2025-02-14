#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : mail.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 14.06.2023

import logging
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import marko
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

    def __init__(self, to: list[str] | None = None,
                       cc: list[str] | None = None,
                       **template_kwargs):
        if to is None:
            raise ValueError('Email must have at least one recipient.')
        self.to = to
        self.cc = [] if cc is None else cc
        self.template_kwargs = template_kwargs
        self.env = Environment(loader=FileSystemLoader(PKG_TEMPLATES / 'emails'))
    
    @property
    def subject(self) -> str:
        raise NotImplemented()

    @property
    def header(self) -> str:
        raise NotImplemented()

    def paragraphs(self) -> Generator[str, None, None]:
        template = self.env.get_template(self.template)
        md = template.render(**self.template_kwargs)
        for element in marko.parse(md).children:
            if (text := marko.render(element).strip()):
                yield text
    
    @property
    def emails(self):
        return self.to
    
    @property
    def ccEmails(self):
        return self.cc


class NewAccountEmail(Email):

    def __init__(self, to: list[str] | None = None,
                       cc: list[str] | None = None,
                       **kwargs):
        self.template = 'account-ready.txt.j2'
        super().__init__(to=to, cc=cc, **kwargs)

    @property
    def subject(self) -> str:
        sitename = self.template_kwargs['sitename']
        return f'UCD HPC: Account Ready on {sitename.capitalize()}'
    
    @property
    def header(self) -> str:
        return 'Your account has been processed'


class UpdateSSHKeyEmail(Email):
    
    def __init__(self, to: list[str] | None = None,
                       cc: list[str] | None = None,
                       **kwargs):
        self.template = 'key-updated.txt.j2'
        super().__init__(to=to, cc=cc, **kwargs)

    @property
    def subject(self) -> str:
        return 'UCD HPC: SSH Key Updated'
    
    @property
    def header(self) -> str:
        return 'Your SSH key update has been processed'


class NewSponsorEmail(Email):

    def __init__(self, to: list[str] | None = None,
                       cc: list[str] | None = None,
                       **kwargs):
        self.template = 'new-sponsor.txt.j2'
        super().__init__(to=to, cc=cc, **kwargs)

    @property
    def subject(self) -> str:
        sitename = self.template_kwargs['sitename']
        return f'UCD HPC: New Sponsored Group on {sitename.capitalize()}'
    
    @property
    def header(self) -> str:
        group = self.template_kwargs['group']
        return f'Your new group <strong>{group}</strong> has been created'


class NewMembershipEmail(Email):

    def __init__(self, to: list[str] | None = None,
                       cc: list[str] | None = None,
                       **kwargs):
        self.template = 'new-membership.txt.j2'
        super().__init__(to=to, cc=cc, **kwargs)

    @property
    def subject(self) -> str:
        sitename = self.template_kwargs['sitename']
        return f'UCD HPC: New Group Membership on {sitename.capitalize()}'
    
    @property
    def header(self) -> str:
        return 'Your new group membership has been processed'