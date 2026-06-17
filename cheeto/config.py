#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : database.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 11.10.2024

from dataclasses import field
import logging
import pathlib
from typing import List, Optional, Union, Mapping

from marshmallow.exceptions import ValidationError
from marshmallow_dataclass import dataclass

from . import __version__
from .yaml import parse_yaml
from .types import *
from .utils import require_kwargs
from .xdg_base_dirs import xdg_config_home


DEFAULT_CONFIG_PATH = xdg_config_home() / 'cheeto' / 'config.yaml'


@require_kwargs
@dataclass(frozen=True)
class LDAPConfig(BaseModel):
    servers: List[str]
    searchbase: str

    user_base: Optional[str] = None
    login_dn: Optional[str] = None
    password: Optional[str] = None

    request_timeout_seconds: float = 10.0
    pool_max_connections: int = 5
    pool_idle_connections: int = 2
    use_tls: bool = False
    auth_mechanism: str = 'SIMPLE'   # 'SIMPLE' | 'GSSAPI' (only SIMPLE wired today)
    gssapi_keytab: Optional[str] = None
    gssapi_realm: Optional[str] = None


@require_kwargs
@dataclass(frozen=True)
class MongoConfig(BaseModel):
    uri: str
    port: int
    user: str
    tls: bool
    password: str
    database: str
    old_database: Optional[str] = None
    tls_ca_file: Optional[str] = None


@require_kwargs
@dataclass(frozen=True)
class HippoConfig(BaseModel):
    api_key: str
    base_url: str
    site_aliases: Mapping[str, str]
    max_tries: int


@require_kwargs
@dataclass(frozen=True)
class IAMConfig(BaseModel):
    api_key: str
    base_url: str
    grace_days: int = 0
    expiry_offset_days: int = 30
    request_timeout_seconds: float = 30.0


@require_kwargs
@dataclass(frozen=True)
class SlurmConfig(BaseModel):
    account_attrs: Mapping[str, str]
    qos_attrs: Mapping[str, str]


# A task schedule: int/float seconds interval, a 5-field crontab string, or
# None/absent to disable the task. int must precede str in the Union so
# numeric YAML values don't deserialize as strings.
ScheduleT = Optional[Union[int, str]]


@require_kwargs
@dataclass(frozen=True)
class HippoTaskConfig(BaseModel):
    schedule: ScheduleT = None
    post_back: bool = True


@require_kwargs
@dataclass(frozen=True)
class IAMSyncTaskConfig(BaseModel):
    # grace_days / expiry_offset_days come from IAMConfig
    schedule: ScheduleT = None
    concurrency: int = 1
    types: Optional[List[str]] = None
    max_users: Optional[int] = None
    # Email users when they are flipped into offboarding.
    notify: bool = True


@require_kwargs
@dataclass(frozen=True)
class LDAPSyncTaskConfig(BaseModel):
    schedule: ScheduleT = None
    sites: Optional[List[str]] = None
    concurrency: int = 1
    max_deletions: int = 50
    prune: bool = True
    force: bool = False
    # Bypass the incremental gate (sync all records via the normal upsert
    # path; force additionally delete-recreates).
    full: bool = False


@require_kwargs
@dataclass(frozen=True)
class SlurmSyncTaskConfig(BaseModel):
    schedule: ScheduleT = None
    sites: Optional[List[str]] = None
    apply: bool = True
    sudo: bool = False
    concurrency: int = 8
    max_deletions: int = 50


@require_kwargs
@dataclass(frozen=True)
class ReapTaskConfig(BaseModel):
    schedule: ScheduleT = None
    # Email users when their account is deactivated at the end of offboarding.
    notify: bool = True


@require_kwargs
@dataclass(frozen=True)
class SympaTaskConfig(BaseModel):
    schedule: ScheduleT = None
    output_dir: str = '/var/lib/cheeto/sympa'
    sites: Optional[List[str]] = None
    ignore: Optional[List[str]] = None


@require_kwargs
@dataclass(frozen=True)
class PuppetSyncTaskConfig(BaseModel):
    # Path to the puppet.hpc repo clone on the hub host (deploy step —
    # the daemon never clones).
    repo: str
    schedule: ScheduleT = None
    sites: Optional[List[str]] = None
    base_branch: str = 'main'
    push: bool = True
    write_keys: bool = True
    delete_branch: bool = True


@require_kwargs
@dataclass(frozen=True)
class DaemonTasksConfig(BaseModel):
    hippo: Optional[HippoTaskConfig] = None
    iam_sync: Optional[IAMSyncTaskConfig] = None
    ldap_sync: Optional[LDAPSyncTaskConfig] = None
    slurm_sync: Optional[SlurmSyncTaskConfig] = None
    reap: Optional[ReapTaskConfig] = None
    sympa: Optional[SympaTaskConfig] = None
    puppet_sync: Optional[PuppetSyncTaskConfig] = None


@require_kwargs
@dataclass(frozen=True)
class DaemonConfig(BaseModel):
    broker_url: str
    sites: List[str]
    author: str = 'cheeto-daemon'
    timezone: str = 'America/Los_Angeles'
    beat_schedule_filename: str = '/var/lib/cheeto/celerybeat-schedule'
    task_time_limit: int = 3600
    tasks: DaemonTasksConfig = field(default_factory=DaemonTasksConfig)


@require_kwargs
@dataclass(frozen=True)
class ApiConfig(BaseModel):
    host: str = '127.0.0.1'
    port: int = 8000
    api_key: Optional[str] = None
    root_path: str = ''


@require_kwargs
@dataclass(frozen=True)
class _Config(BaseModel):
    ldap: Mapping[str, LDAPConfig]
    hippo: HippoConfig
    ucdiam: IAMConfig
    mongo: Mapping[str, MongoConfig]
    daemon: Optional[Mapping[str, DaemonConfig]] = None
    api: Optional[Mapping[str, ApiConfig]] = None


@require_kwargs
@dataclass(frozen=True)
class Config(BaseModel):
    ldap: LDAPConfig
    hippo: HippoConfig
    ucdiam: IAMConfig
    mongo: MongoConfig
    daemon: Optional[DaemonConfig] = None
    api: Optional[ApiConfig] = None


def _profiled(section: Optional[Mapping], profile: str):
    """Resolve a profiled config section, falling back to 'default'; None if
    the section is absent from the config file."""
    if not section:
        return None
    return section.get(profile, section.get('default'))


def get_config(config_path: Optional[pathlib.Path] = None,
               profile: str = 'default') -> Union[Config, None]:
    logger = logging.getLogger(__name__)

    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    config_yaml = parse_yaml(str(config_path))

    try:
        config : _Config = _Config.Schema().load(config_yaml)
    except ValidationError as e: #type: ignore
        logger.error(f'[red]ValidationError loading config: {config_path}[/]')
        logger.error(e.messages)
        return None
    else:
        return Config(
            ldap = config.ldap.get(profile, config.ldap['default']),
            mongo = config.mongo.get(profile, config.mongo['default']),
            hippo = config.hippo,
            ucdiam = config.ucdiam,
            daemon = _profiled(config.daemon, profile),
            api = _profiled(config.api, profile)
        )
