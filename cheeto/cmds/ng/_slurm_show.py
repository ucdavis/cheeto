"""Display-dict wrappers around cheeto.queries.slurm for the show commands.

The async DB access lives in `cheeto.queries.slurm`; this module converts
its results into the dict shape consumed by the Rich/YAML renderers.
"""

from __future__ import annotations

from ...models.group import Group
from ...models.site import Site
from ...models.slurm import SlurmAccount, SlurmAssociation, SlurmTRES
from ...models.user import User
from ...queries.slurm import (
    GroupSlurm,
    group_slurm_at_site as _query_group_slurm,
    total_tres,
    user_slurm_at_site as _query_user_slurm,
)


def _tres_compact(tres: SlurmTRES) -> str:
    """Compact display: '128c/8g/1T', omits fields at their unlimited default."""
    parts: list[str] = []
    if tres.cpus is not None:
        parts.append(f'{tres.cpus}c')
    if tres.gpus is not None:
        parts.append(f'{tres.gpus}g')
    if tres.mem is not None:
        parts.append(str(tres.mem))
    return '/'.join(parts) if parts else '\u221e'   # ∞


def _account_dict(account: SlurmAccount) -> dict:
    return {
        'limits': {
            'max_user_jobs': account.limits.max_user_jobs,
            'max_group_jobs': account.limits.max_group_jobs,
            'max_submit_jobs': account.limits.max_submit_jobs,
            'max_job_length': account.limits.max_job_length,
        },
        'coordinators': sorted(c.name for c in account.coordinators),
    }


def _assoc_dict(assoc: SlurmAssociation) -> dict:
    return {
        'partition': assoc.partition.name,
        'qos': assoc.qos.name,
        'qos_priority': assoc.qos.priority,
        'qos_flags': list(assoc.qos.flags),
        'qos_total_tres': _tres_compact(total_tres(assoc.qos.group_limits)),
    }


def _group_slurm_dict(info: GroupSlurm | None) -> dict | None:
    if info is None:
        return None
    return {
        'account': _account_dict(info.account),
        'associations': [_assoc_dict(a) for a in info.associations],
    }


async def group_slurm_at_site(group: Group, site: Site) -> dict | None:
    """Return a display dict for the (group, site) Slurm info, or None."""
    return _group_slurm_dict(await _query_group_slurm(group, site))


async def user_slurm_at_site(user: User, site: Site) -> list[dict]:
    """Return per-(group, role) display dicts for Slurm info the user can access."""
    entries = await _query_user_slurm(user, site)
    return [
        {
            'group': e.group.name,
            'role': e.role,
            'slurm': _group_slurm_dict(e.slurm),
        }
        for e in entries
    ]
