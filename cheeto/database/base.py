#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : database/base.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

from functools import singledispatchmethod
from typing import Mapping
import uuid

from mongoengine import (
    connect, 
    Document, 
    QuerySet, 
    GenericReferenceField, 
    ReferenceField, 
    EmbeddedDocument,
    ListField
)

from ..config import MongoConfig
from ..log import Console
from ..types import is_listlike
from ..yaml import dumps as dumps_yaml


def handler(event):
    """Signal decorator to allow use of callback functions as class decorators."""

    def decorator(fn):
        def apply(cls):
            event.connect(fn, sender=cls)
            return cls

        fn.apply = apply
        return fn

    return decorator


class BaseView:

    def __init__(self, document: Document,
                       strip_id: bool = True,
                       strip_empty: bool = False,):
        self.document = document

    def __getattr__(self, key: str):
        if key in self.document._fields:
            return getattr(self.document, key)

    @singledispatchmethod
    def viewattr(self, attr: object):
        return attr
    
    @viewattr.register
    def _(self, attr: ListField):
        pass
    
    def _pretty(self, formatters: Mapping[str, str] | None = None,
                      lift: list[str] | None = None,
                      skip: tuple | None = None,
                      order: list[str] | None = None,
                      extra: dict | None = None) -> str:
        return self.document._pretty(formatters=formatters,
                                     lift=lift,
                                     skip=skip,
                                     order=order,
                                     extra=extra)

    def pretty(self, formatters: Mapping[str, str] | None = None,
                     lift: list[str] | None = None,
                     skip: tuple | None = None,
                     order: list[str] | None = None) -> str:
        return self.document.pretty(formatters=formatters,
                                    lift=lift,
                                    skip=skip,
                                    order=order)



class BaseDocument(Document):
    meta = {
        'abstract': True
    }

    def to_dict(self, strip_id=True, strip_empty=False, raw=False, rekey=False, **kwargs):
        data = self.to_mongo(use_db_field=False).to_dict()
        if strip_id:
            data.pop('id', None)
            data.pop('_id', None)
        for key in list(data.keys()):
            if strip_empty:
                if not data[key] and data[key] != 0:
                    del data[key]
            if not raw and key in data \
               and isinstance(self._fields[key], (ReferenceField, GenericReferenceField)):
                data[key] = getattr(self, key).to_dict(strip_empty=strip_empty,
                                                       raw=raw,
                                                       strip_id=strip_id,
                                                       rekey=rekey)
            if rekey and key in data and key.startswith('_'):
                data[key.lstrip('_') ] = data[key]
                del data[key]
        return data

    def clean(self):
        if 'sitename' in self._fields: #type: ignore
            from .crud import query_site_exists
            query_site_exists(self.sitename) #type: ignore

    def __repr__(self):
        return dumps_yaml(self.to_dict(raw=True, strip_id=False, strip_empty=False))

    def _pretty(self, formatters: Mapping[str, str] | None = None,
                      lift: list[str] | None = None,
                      skip: tuple | None = None,
                      order: list[str] | None = None,
                      extra: dict | None = None) -> str:
        if formatters is None:
            formatters = {}
        if skip is None:
            skip = tuple()
        data = apply_formatters(self, formatters, skip)
        if lift:
            lift_values(data, lift)
        if order is None:
            order = []
        if extra is not None:
            data.update(extra)
        _data = {key: data[key] for key in order if key in data}
        _data.update({key: data[key] for key in sorted(data.keys()) if key not in order})
        return _data

    def pretty(self, formatters: Mapping[str, str] | None = None,
                     lift: list[str] | None = None,
                     skip: tuple | None = None,
                     order: list[str] | None = None) -> str:
        return dumps_yaml(self._pretty(formatters=formatters,
                                       lift=lift,
                                       skip=skip,
                                       order=order))


def apply_formatters(doc: BaseDocument,
                     formatters: Mapping[str, str],
                     skip: tuple) -> dict:
    formatted = {}
    for key in doc._data.keys():
        if key in skip or key in ('_id', 'id'):
            continue
        value = getattr(doc, key)
        if is_listlike(value):
            value = list(value)
        if value is not False and not value:
            formatted[key] = None
        elif key in formatters:
            if is_listlike(value):
                formatted[key] = sorted([formatters[key].format(data=d) for d in value])
            else:
                formatted[key] = formatters[key].format(data=value)
        elif key not in formatters and isinstance(value, (BaseDocument, EmbeddedDocument)):
            formatted[key] = apply_formatters(value, formatters, skip)
        else:
            formatted[key] = value
    return {key.lstrip('_'): value for key, value in formatted.items() if value is not None}


def lift_values(data: dict, keys: list[str]) -> list:
    for to_lift in keys:
        if to_lift in data and isinstance(data[to_lift], dict):
            for key, value in data[to_lift].items():
                if key not in data:
                    data[key] = value
            del data[to_lift]


class SyncQuerySet(QuerySet):
    
    def update(self, *args, **kwargs):
        kwargs['ldap_synced'] = kwargs.get('ldap_synced', False)
        return super().update(*args, **kwargs)

    def update_one(self, *args, **kwargs):
        kwargs['ldap_synced'] = kwargs.get('ldap_synced', False)
        return super().update_one(*args, **kwargs)


def connect_to_database(config: MongoConfig, quiet: bool = False):
    if not quiet:
        console = Console(stderr=True)
        console.print(f'mongo config:')
        console.print(f'  uri: [green]{config.uri}:{config.port}')
        console.print(f'  user: [green]{config.user}')
        console.print(f'  db: [green]{config.database}')
        console.print(f'  tls: {config.tls}')
    if config.user:
        return connect(config.database,
                       host=f'{config.uri}:{config.port}',
                       username=config.user,
                       password=config.password,
                       tls=config.tls,
                       uuidRepresentation='standard')
    else:
        return connect(config.database,
                       host=f'{config.uri}:{config.port}',
                       tls=config.tls,
                       uuidRepresentation='standard')

