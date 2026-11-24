"""Response models of the read-only REST API.

These are the public contract (FastAPI publishes them as the OpenAPI
schema), kept apart from the beanie documents on purpose: a document field
does not reach a client unless a model here names it. The password hash,
IAM data, LDAP bookkeeping and operator comments (`User.comments`, the
`comment` of a Slurm or storage allocation) are never exposed. No model
here may have a field named `comment` or `comments`; a test enforces it.

Conventions:
- Other entities are referenced by name (`group: 'mylab'`), never by id.
- `status` / `access` are the shorthands (`active`, `login-ssh`).
- In a TRES, `null` means unlimited.
"""

# pydantic resolves the annotations below through this module's namespace;
# under lazy imports (PEP 810) a name that was never touched is still a
# proxy there. See the lazy-import rules in CLAUDE.md.
__eager_imports__ = True

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Sites
# ---------------------------------------------------------------------------


class SiteSummary(BaseModel):
    name: str
    fqdn: str
    aliases: list[str] = Field(default_factory=list)


class SiteDetail(SiteSummary):
    sticky_groups: list[str] = Field(
        default_factory=list,
        description='Groups that every user at the site is a member of.',
    )
    sticky_slurm_accounts: list[str] = Field(
        default_factory=list,
        description='Slurm accounts that every eligible user at the site '
                    'can use.',
    )
    default_slurm_account: str | None = None


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


class StorageOut(BaseModel):
    name: str
    category: str = Field(description='home, group or share')
    owner: str
    group: str
    volume: str
    host: str
    host_path: str
    subpath: str | None = None
    quota: str | None = None
    mount_type: Literal['automount', 'static'] | None = None
    mount_source: str | None = Field(
        default=None,
        description='Name of the automount map or the static mount.',
    )
    mount_path: str | None = None
    mount_options: list[str] = Field(default_factory=list)
    globus: bool = False


class VolumeOut(BaseModel):
    name: str
    backend: str
    host: str
    host_path: str
    quota: str | None = None
    parent: str | None = None
    zfs_dataset: str | None = None
    provisioned_at: datetime | None = None


class AutomountMapOut(BaseModel):
    name: str
    prefix: str
    options: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Slurm
# ---------------------------------------------------------------------------


class TRES(BaseModel):
    cpus: int | None = None
    gpus: int | None = None
    mem: str | None = None


class AllocationOut(BaseModel):
    # No `comment`: operator comments are never exposed (see the module
    # docstring and the `test_no_comment_*` tests in cheeto/tests/test_api.py).
    tres: TRES
    expires_at: datetime | None = None


class QOSOut(BaseModel):
    name: str
    priority: int | None = None
    flags: list[str] = Field(default_factory=list)
    group_limits: list[AllocationOut] = Field(default_factory=list)
    user_limits: list[AllocationOut] = Field(default_factory=list)
    job_limits: list[AllocationOut] = Field(default_factory=list)
    group_total: TRES = Field(description='Sum of group_limits (GrpTRES).')
    user_total: TRES = Field(description='Sum of user_limits (MaxTRESPerUser).')
    job_total: TRES = Field(description='Sum of job_limits (MaxTRESPerJob).')


class QOSAllocationOut(AllocationOut):
    qos: str
    limit: Literal['group_limits', 'user_limits', 'job_limits']


class SlurmAccountLimitsOut(BaseModel):
    max_user_jobs: int = -1
    max_group_jobs: int = -1
    max_submit_jobs: int = -1
    max_job_length: str = '-1'


class SlurmAccountOut(BaseModel):
    name: str = Field(description='The owning group; also the account name.')
    limits: SlurmAccountLimitsOut
    coordinators: list[str] = Field(default_factory=list)


class AssociationOut(BaseModel):
    """A configured (account, partition, qos) row. Users are not part of
    it; see `UserAssociationOut` for the effective per-user rows."""
    account: str
    partition: str
    qos: str


class PartitionQOS(BaseModel):
    partition: str
    qos: str


class UserAssociationOut(BaseModel):
    """An effective association: what the Slurm sync creates."""
    user: str
    account: str
    partition: str
    qos: str
    max_jobs: int = Field(description='-1: no limit. 0: disabled user.')


class SiteSlurmUsers(BaseModel):
    associations: list[UserAssociationOut] = Field(default_factory=list)
    default_accounts: dict[str, str] = Field(
        default_factory=dict, description='user -> default account',
    )
    coordinators: dict[str, list[str]] = Field(
        default_factory=dict, description='account -> coordinator users',
    )


class GroupSlurmOut(BaseModel):
    account: SlurmAccountOut
    associations: list[PartitionQOS] = Field(default_factory=list)


class UserSlurmAccount(BaseModel):
    account: str
    via: list[str] = Field(
        description='How the user reaches the account: member, slurmer, '
                    'sticky-member (sticky group) or sticky (sticky account).',
    )
    associations: list[PartitionQOS] = Field(default_factory=list)


class UserSlurmOut(BaseModel):
    eligible: bool = Field(
        description='True when the user holds Slurm associations at the '
                    'site: effective access includes slurm and the '
                    'effective status allows it. When false, `accounts` '
                    'lists what the user would get.',
    )
    max_jobs: int | None = Field(
        default=None,
        description='MaxJobs of the associations; null when not eligible.',
    )
    default_account: str | None = None
    accounts: list[UserSlurmAccount] = Field(default_factory=list)


class SiteUserSlurm(BaseModel):
    eligible: bool
    max_jobs: int | None = None
    accounts: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


class UserSummary(BaseModel):
    name: str
    uid: int
    gid: int
    email: str
    fullname: str
    type: str
    status: str | None = Field(default=None, description='Global status.')
    access: list[str] = Field(
        default_factory=list, description='Global access list.',
    )


class UserOut(UserSummary):
    shell: str
    home_directory: str
    has_password: bool
    expires_at: datetime | None = None
    sites: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SshKeysOut(BaseModel):
    user: str
    keys: list[str] = Field(default_factory=list)


class UserSiteOut(BaseModel):
    site: str
    status_override: str | None = Field(
        default=None,
        description='Status set for this site only; null: global status.',
    )
    access_override: list[str] = Field(
        default_factory=list,
        description='Access list set for this site only; empty: global '
                    'access list.',
    )
    status: str | None = Field(default=None, description='Effective status.')
    access: list[str] = Field(
        default_factory=list, description='Effective access list.',
    )
    expires_at: datetime | None = Field(
        default=None,
        description='Effective expiry: the site value, else the global one.',
    )


class UserGroupOut(BaseModel):
    name: str
    gid: int
    roles: list[str] = Field(
        default_factory=list,
        description='Explicit roles: member, sponsor, sudoer, slurmer.',
    )
    sticky: bool = Field(
        default=False,
        description='True: every user at the site is a member.',
    )


class UserStorageOut(BaseModel):
    owned: list[StorageOut] = Field(default_factory=list)
    group: list[StorageOut] = Field(
        default_factory=list,
        description='Storages of the groups the user is a member of, '
                    'excluding the ones the user owns.',
    )


class SiteUserOut(BaseModel):
    """One user in the site-wide list: the global record plus everything
    that is specific to the site."""
    name: str
    uid: int
    gid: int
    email: str
    fullname: str
    type: str
    shell: str
    home_directory: str
    has_password: bool
    status: str | None = Field(default=None, description='Effective status.')
    access: list[str] = Field(
        default_factory=list, description='Effective access list.',
    )
    expires_at: datetime | None = None
    groups: list[UserGroupOut] = Field(default_factory=list)
    slurm: SiteUserSlurm
    home: StorageOut | None = None


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------


class GroupSummary(BaseModel):
    name: str
    gid: int
    type: str


class GroupOut(GroupSummary):
    sites: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class GroupRoster(BaseModel):
    members: list[str] = Field(default_factory=list)
    sponsors: list[str] = Field(default_factory=list)
    sudoers: list[str] = Field(default_factory=list)
    slurmers: list[str] = Field(default_factory=list)


class GroupSiteOut(GroupSummary):
    site: str
    sticky: bool = False
    slurm_account: bool = False
    members: int = 0
    sponsors: int = 0
    sudoers: int = 0
    slurmers: int = 0


class SiteGroupOut(GroupSummary, GroupRoster):
    sticky: bool = False
    slurm_account: bool = False
    storages: list[StorageOut] = Field(default_factory=list)
