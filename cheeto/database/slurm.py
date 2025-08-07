#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/slurm.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

from typing import no_type_check, Self

from mongoengine import (IntField,
                         ListField,
                         ReferenceField,
                         StringField,
                         EmbeddedDocument,
                         EmbeddedDocumentField,
                         CASCADE)

from ..puppet import (SlurmQOSTRES as PuppetSlurmQOSTRES,
                      SlurmQOS as PuppetSlurmQOS)
from ..utils import size_to_megs

from .base import BaseDocument
from .fields import (POSIXNameField,
                     DataQuotaField,
                     SlurmQOSFlagField,
                     GroupTypeField)
from .group import SiteGroup


class SiteSlurmPartition(BaseDocument):
    partitionname = POSIXNameField(required=True)
    sitename = StringField(required=True, unique_with='partitionname')


class SlurmTRES(EmbeddedDocument):
    cpus = IntField(default=-1)
    gpus = IntField(default=-1)
    mem = DataQuotaField()

    def to_slurm(self) -> str:
        tokens = [f'cpu={self.cpus}',
                  f'mem={size_to_megs(self.mem) if self.mem is not None else -1}', #type: ignore
                  f'gres/gpu={self.gpus}']
        return ','.join(tokens)

    @staticmethod
    def negate() -> str:
        return 'cpu=-1,mem=-1,gres/gpu=-1'

    def clean(self):
        if self.mem == -1:
            self.mem = None
        elif self.mem is not None:
            self.mem = f'{size_to_megs(self.mem)}M' #type: ignore

    @classmethod
    def from_puppet(cls, puppet_tres: PuppetSlurmQOSTRES) -> Self:
        return cls(cpus=puppet_tres.cpus,
                   gpus=puppet_tres.gpus,
                   mem=puppet_tres.mem)

    def to_puppet(self) -> PuppetSlurmQOSTRES:
        return PuppetSlurmQOSTRES(cpus=None if self.cpus == -1 else self.cpus,
                                  gpus=None if self.gpus == -1 else self.gpus,
                                  mem=self.mem)

    def to_dict(self):
        data = self.to_mongo(use_db_field=False).to_dict()
        data.pop('id', None)
        data.pop('_id', None)
        return data

    def update_from_dict(self, data: dict):
        if 'cpus' in data and data['cpus'] is not None:
            self.cpus = data['cpus']
        if 'gpus' in data and data['gpus'] is not None:
            self.gpus = data['gpus']
        if 'mem' in data and data['mem'] is not None:
            self.mem = data['mem']


class SiteSlurmQOS(BaseDocument):
    sitename = StringField(required=True)
    qosname = StringField(required=True, unique_with='sitename')
    group_limits = EmbeddedDocumentField(SlurmTRES, default=lambda: SlurmTRES())
    user_limits = EmbeddedDocumentField(SlurmTRES, default=lambda: SlurmTRES())
    job_limits = EmbeddedDocumentField(SlurmTRES, default=lambda: SlurmTRES())
    priority = IntField()
    flags = ListField(SlurmQOSFlagField())

    PRETTY_ORDER = ['qosname', 'sitename', 'group_limits', 'user_limits', 'job_limits', 'priority', 'flags']

    @property
    def group(self):
        return self.group_limits

    @property
    def user(self):
        return self.user_limits

    @property
    def job(self):
        return self.job_limits

    @no_type_check
    def to_slurm(self, modify: bool = False) -> list[str]:
        tokens = []
        grptres = self.group_limits.to_slurm() \
            if self.group_limits is not None else SlurmTRES.negate() 
        usertres = self.user_limits.to_slurm() \
            if self.user_limits is not None else SlurmTRES.negate()
        jobtres = self.job_limits.to_slurm() \
            if self.job_limits is not None else SlurmTRES.negate()
        flags = ','.join(self.flags) if self.flags else ('-1' if modify else None)
        
        tokens.append(f'GrpTres={grptres}')
        tokens.append(f'MaxTRESPerUser={usertres}')
        tokens.append(f'MaxTresPerJob={jobtres}')
        if flags:
            tokens.append(f'Flags={flags}')
        tokens.append(f'Priority={self.priority}')

        return tokens

    @no_type_check
    def to_puppet(self):
        return PuppetSlurmQOS(group=self.group_limits.to_puppet() if self.group_limits else None,
                              user=self.user_limits.to_puppet() if self.user_limits else None,
                              job=self.job_limits.to_puppet() if self.job_limits else None,
                              priority=self.priority if self.priority else 0,
                              flags=self.flags if self.flags else None)

    @classmethod
    def from_puppet(cls, qosname: str, sitename: str, puppet_qos: PuppetSlurmQOS) -> Self:
        return cls(qosname=qosname,
                   sitename=sitename,
                   group_limits=SlurmTRES.from_puppet(puppet_qos.group) \
                       if puppet_qos.group is not None else None,
                   user_limits=SlurmTRES.from_puppet(puppet_qos.user) \
                       if puppet_qos.user is not None else None,
                   job_limits=SlurmTRES.from_puppet(puppet_qos.job) \
                       if puppet_qos.job is not None else None,
                   priority = puppet_qos.priority,
                   flags = list(puppet_qos.flags) \
                       if puppet_qos.flags is not None else None)


class SiteSlurmAssociation(BaseDocument):
    sitename = StringField(required=True)
    qos = ReferenceField(SiteSlurmQOS, required=True, reverse_delete_rule=CASCADE)
    partition = ReferenceField(SiteSlurmPartition, required=True, reverse_delete_rule=CASCADE)
    group = ReferenceField(SiteGroup, required=True, reverse_delete_rule=CASCADE)

    def to_dict(self, strip_id=True, strip_empty=False, raw=False, **kwargs):
        data = super().to_dict(strip_id=strip_id, strip_empty=strip_empty, raw=raw, **kwargs)
        if not raw:
            data['qos'] = self.qos.to_dict(strip_id=strip_id) #type: ignore
            data['partition'] = self.partition.partitionname #type: ignore
            data['group'] = self.group.groupname #type: ignore
        return data
