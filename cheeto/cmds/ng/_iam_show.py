"""Display helpers for the IAM offboarding state shown by `ng user show` and
`ng iam show`.

The commands do the DB access; this module derives the offboarding phase
from a user's stored IAM bookkeeping plus their status and `expires_at`, and
renders it as rows on a two-column Rich table. The phase logic mirrors the
state machine in `cheeto.operations.iam` (`SyncUserIAM` and
`ReapOffboardedUsers`) but performs no I/O.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from rich.table import Table

from ...models.user import UCDIAMInfo
from ...operations.iam import _naive_utc


def fmt_remaining(td: timedelta) -> str:
    """Render a duration as 'Nd Nh'. Negative durations clamp to '0d 0h'."""
    total = max(int(td.total_seconds()), 0)
    days, rem = divmod(total, 86400)
    return f'{days}d {rem // 3600}h'


def iam_offboarding(
    iam: UCDIAMInfo | None,
    *,
    status: str | None,
    expires_at: datetime | None,
    grace_days: int,
    expiry_offset_days: int,
    now: datetime | None = None,
) -> dict | None:
    """Derive where a user sits in the IAM offboarding pipeline.

    Returns None when there is nothing to report: no IAM data, or present in
    IAM with a status the pipeline does not touch (active/disabled).
    Otherwise a dict with a 'phase' and the dates that are true for that
    phase. All datetimes are naive UTC, matching the stored fields, so they
    render the same as `created_at` and friends.

    Phases (see the state machine in `cheeto.operations.iam`):

      in_grace      missing, active, grace clock still running
      grace_ended   missing, active, grace ran out but no sync has flipped
                    the user yet (sync lag)
      expiring      missing, active, operator already set a future expires_at
                    (the sync leaves it alone: `miss_already_expiring`)
      offboarding   missing, status offboarding, expires_at set by the sync
      inactive      missing, reaped; expires_at kept as the deactivation date
      disabled      missing, operator-disabled; never auto-offboarded
      restoring     present again while offboarding; next sync restores
      restorable    present again while inactive; not auto-restored
      missing       missing with no first_missing_at (legacy/test data)
    """
    if iam is None:
        return None
    now = _naive_utc(now)
    status = status or ''
    if expires_at is not None:
        # replace() yields a fresh object: the caller also dumps the user's
        # own expires_at, and the YAML emitter would otherwise alias the
        # second occurrence (`deactivates_at: *id001`).
        expires_at = _naive_utc(expires_at).replace()

    if iam.iam_status == 'present':
        if status == 'offboarding':
            return {'phase': 'restoring'}
        if status == 'inactive':
            return {'phase': 'restorable', 'deactivated_at': expires_at}
        return None

    fma = iam.first_missing_at
    if fma is None:
        return {'phase': 'missing'}
    grace_ends = _naive_utc(fma) + timedelta(days=grace_days)
    in_grace = now < grace_ends
    grace_key = 'grace_ends_at' if in_grace else 'grace_ended_at'

    if status == 'offboarding':
        out: dict = {
            'phase': 'offboarding',
            'grace_ended_at': grace_ends,
            'deactivates_at': expires_at,
            'deactivates_in': None,
            'reap_pending': False,
        }
        if expires_at is not None:
            if expires_at > now:
                out['deactivates_in'] = fmt_remaining(expires_at - now)
            else:
                out['reap_pending'] = True
        return out
    if status == 'inactive':
        return {'phase': 'inactive', 'deactivated_at': expires_at}
    if status == 'disabled':
        return {'phase': 'disabled', grace_key: grace_ends}

    # active (or unset status): the grace clock decides what happens next.
    if expires_at is not None and expires_at > now:
        out = {'phase': 'expiring', 'expires_at': expires_at, grace_key: grace_ends}
        if in_grace:
            out['grace_remaining'] = fmt_remaining(grace_ends - now)
        return out
    projected = grace_ends + timedelta(days=expiry_offset_days)
    if in_grace:
        return {
            'phase': 'in_grace',
            'grace_ends_at': grace_ends,
            'grace_remaining': fmt_remaining(grace_ends - now),
            'projected_expires_at': projected,
        }
    return {
        'phase': 'grace_ended',
        'grace_ended_at': grace_ends,
        'projected_expires_at': projected,
    }


def _grace_clause(ob: dict) -> str:
    """'in grace — Nd Nh remaining' or 'grace ended <dt>', whichever is true."""
    if 'grace_ends_at' in ob:
        return f'in grace — {ob["grace_remaining"]} remaining'
    return f'grace ended {ob["grace_ended_at"]}'


def add_offboarding_rows(table: Table, offboarding: dict | None) -> None:
    """Append the offboarding rows for one `iam_offboarding()` result to a
    two-column (label, value) Rich table. Does nothing for None."""
    if not offboarding:
        return
    ob = offboarding
    phase = ob['phase']

    if phase == 'in_grace':
        table.add_row(
            'offboarding',
            f'[yellow]{_grace_clause(ob)}, still active[/]',
        )
        table.add_row('grace ends', str(ob['grace_ends_at']))
        table.add_row(
            'projected expires_at',
            f'[red]{ob["projected_expires_at"]}[/] '
            '(set by the first sync after grace ends)',
        )
    elif phase == 'grace_ended':
        table.add_row(
            'offboarding',
            f'[red]grace ended {ob["grace_ended_at"]} — next sync sets '
            'expires_at and status offboarding[/]',
        )
        table.add_row(
            'projected expires_at',
            f'[red]{ob["projected_expires_at"]}[/] or later',
        )
    elif phase == 'expiring':
        table.add_row(
            'offboarding',
            f'[yellow]{_grace_clause(ob)}; expires_at already set, '
            'sync leaves it[/]',
        )
    elif phase == 'offboarding':
        table.add_row('offboarding', f'[red]grace ended {ob["grace_ended_at"]}[/]')
        if ob['deactivates_at'] is None:
            table.add_row('deactivates', '[red]expires_at not set[/]')
        elif ob['reap_pending']:
            table.add_row(
                'deactivates',
                f'{ob["deactivates_at"]} (expires_at) — '
                '[red]passed; next reap sets inactive[/]',
            )
        else:
            table.add_row(
                'deactivates',
                f'{ob["deactivates_at"]} (expires_at) — '
                f'in {ob["deactivates_in"]}',
            )
    elif phase == 'inactive':
        when = ob.get('deactivated_at')
        tail = f' — deactivated {when} (expires_at)' if when else ''
        table.add_row('offboarding', f'[dim]complete{tail}[/]')
    elif phase == 'disabled':
        table.add_row(
            'offboarding',
            f'[dim]skipped — status disabled is operator-set '
            f'({_grace_clause(ob)})[/]',
        )
    elif phase == 'restoring':
        table.add_row(
            'offboarding',
            '[green]present in IAM again — next sync restores active '
            'and clears expires_at[/]',
        )
    elif phase == 'restorable':
        when = ob.get('deactivated_at')
        tail = f' — deactivated {when}' if when else ''
        table.add_row(
            'offboarding',
            f'[yellow]complete{tail}; present in IAM again, not '
            'auto-restored (see ng iam restorable)[/]',
        )
    elif phase == 'missing':
        table.add_row(
            'offboarding',
            '[red]missing — first_missing_at not recorded[/]',
        )
