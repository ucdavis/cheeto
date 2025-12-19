#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : log.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 23.05.2023

from enum import Enum
import logging
from typing import TextIO

from rich.console import Console as _Console
from rich.logging import RichHandler


def setup(log_file: TextIO,
          level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(funcName)s: %(message)s',
        datefmt="[%x %X]",
        handlers=[RichHandler(console=Console(file=log_file))]
    )


class Emotes(Enum):
    ERROR = '💀'
    WARN = '🚩'
    INFO = '🔔'
    DEBUG = '🐛'
    SUCCESS = '✅'
    FAIL = '❌'
    WAIT = '⏳'
    DONE = '🎉'
    STOP = '🛑'
    START = '🏁'
    QUESTION = '❓'
    EXCLAMATION = '❗'


class Console(_Console):

    def __init__(self, *args, stderr=True, **kwargs):
        super().__init__(*args, soft_wrap=True, stderr=stderr, **kwargs)

    def error(self, *args, **kwargs):
        self.print(Emotes.ERROR.value, *args, style='italic bold red', **kwargs)

    def warn(self, *args, **kwargs):
        self.print(Emotes.WARN.value, *args, style='italic dark_orange', **kwargs)

    def info(self, *args, **kwargs):
        self.print(Emotes.INFO.value, *args, style='italic blue', **kwargs)
    
    def success(self, *args, **kwargs):
        self.print(Emotes.SUCCESS.value, *args, style='italic green', **kwargs)
    
    def fail(self, *args, **kwargs):
        self.print(Emotes.FAIL.value, *args, style='italic red', **kwargs)
    
    def wait(self, *args, **kwargs):
        self.print(Emotes.WAIT.value, *args, style='italic yellow', **kwargs)
    
    def done(self, *args, **kwargs):
        self.print(Emotes.DONE.value, *args, style='italic green', **kwargs)