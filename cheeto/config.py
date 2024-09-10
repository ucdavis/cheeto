
import argparse
import logging
import pathlib
import sys
from typing import List, Optional, Union, Mapping

from marshmallow import post_dump
from marshmallow.exceptions import ValidationError
from marshmallow_dataclass import dataclass

from .args import subcommand
from .errors import ExitCode
from .yaml import parse_yaml
from .types import *
from .utils import require_kwargs
from .xdg_base_dirs import xdg_config_home


@require_kwargs
@dataclass(frozen=True)
class LDAPConfig(BaseModel):
    servers: List[str]
    searchbase: str

    user_status_groups: Mapping[str, str]

    user_classes: List[str]
    user_attrs: Mapping[str, str]
    user_base: Optional[str] = None

    login_dn: Optional[str] = None
    password: Optional[str] = None

    group_classes: Optional[List[str]] = None
    group_attrs: Optional[Mapping[str, str]] = None



@require_kwargs
@dataclass(frozen=True)
class MongoConfig(BaseModel):
    uri: str
    port: int
    user: str
    password: str
    database: str


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


@require_kwargs
@dataclass(frozen=True)
class Config(BaseModel):
    ldap: Mapping[str, LDAPConfig]
    hippo: HippoConfig
    ucdiam: IAMConfig
    mongo: MongoConfig


def get_config_path() -> pathlib.Path:
    return xdg_config_home() / 'cheeto' / 'config.yaml'


def get_config(config_path: Optional[pathlib.Path] = None) -> Union[Config, None]:
    logger = logging.getLogger(__name__)

    if config_path is None:
        config_path = get_config_path()
    config_yaml = parse_yaml(str(config_path))

    try:
        config = Config.Schema().load(config_yaml)
    except ValidationError as e: #type: ignore
        logger.error(f'[red]ValidationError loading config: {config_path}[/]')
        logger.error(e.messages)
        return None
    else:
        return config #type: ignore


def add_show_args(parser):
    pass


@subcommand('show', add_show_args)
def show(args: argparse.Namespace):
    logger = logging.getLogger(__name__)

    if args.config is None:
        sys.exit(ExitCode.VALIDATION_ERROR)
    else:
        print(Config.Schema().dumps(args.config))


def add_write_args(parser):
    pass


@subcommand('write', add_write_args)
def write(args: argparse.Namespace):
    logger = logging.getLogger(__name__)

    config = Config(ldap = dict(
                        hpccf = LDAPConfig(servers=['ldaps://ldap1.hpc.ucdavis.edu', 'ldaps://ldap2.hpc.ucdavis.edu'],
                                           searchbase='dc=hpc,dc=ucdavis,dc=edu',
                                           login_dn='uid=cheeto,ou=Services,dc=hpc,dc=ucdavis,dc=edu',
                                           password='password',
                                           user_classes=['inetOrgPerson', 'posixAccount']),
                        ucdavis = LDAPConfig(servers=['ldaps://ldap.ucdavis.edu'],
                                             searchbase='ou=People,dc=ucdavis,dc=edu',
                                             login_dn='',
                                             password='',
                                             user_classes=['inetOrgPerson'])
                    ))

    config_path = get_config_path()
    if not config_path.exists():
        logger.info(f'Config file not found, writing basic config to {config_path}')
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(Config.Schema().dumps(config))
    else:
        logger.warn(f'Config file already exists at {config_path}, exiting.')
        sys.exit(0)
