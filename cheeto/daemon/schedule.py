#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : daemon/schedule.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.06.2026

from celery.schedules import crontab

from ..config import Config


def parse_schedule(value: int | float | str) -> float | crontab:
    """A numeric schedule is an interval in seconds; a string is a 5-field
    crontab (minute hour day-of-month month day-of-week)."""
    if isinstance(value, (int, float)):
        return float(value)
    fields = value.split()
    if len(fields) != 5:
        raise ValueError(
            f'Invalid crontab schedule {value!r}: need 5 fields '
            '(minute hour day-of-month month day-of-week)'
        )
    minute, hour, dom, month, dow = fields
    return crontab(minute=minute, hour=hour, day_of_month=dom,
                   month_of_year=month, day_of_week=dow)


def _options(schedule: int | float | str, queue: str | None = None) -> dict:
    opts: dict = {}
    if queue is not None:
        opts['queue'] = queue
    # Interval tasks that can't be delivered within one interval are stale —
    # expire them so a backed-up queue drops ticks instead of piling them up.
    # Crontab tasks (typically daily) must not be silently dropped.
    if isinstance(schedule, (int, float)):
        opts['expires'] = float(schedule)
    return opts


def build_beat_schedule(config: Config) -> dict[str, dict]:
    """Build the celery beat_schedule dict from the daemon config: one entry
    per enabled singleton task (hippo, iam_sync, reap) and one per site for
    the fan-out tasks (ldap_sync, sympa, slurm_sync). slurm_sync entries are
    routed to the per-site `slurm.<site>` queue consumed by that cluster's
    head-node worker; everything else lands on the default `cheeto` queue."""
    daemon = config.daemon
    tasks = daemon.tasks
    sched: dict[str, dict] = {}

    if tasks.hippo is not None and tasks.hippo.schedule is not None:
        sched['hippo-process'] = {
            'task': 'cheeto.hippo_process',
            'schedule': parse_schedule(tasks.hippo.schedule),
            'options': _options(tasks.hippo.schedule),
        }
    if tasks.iam_sync is not None and tasks.iam_sync.schedule is not None:
        sched['iam-sync'] = {
            'task': 'cheeto.iam_sync',
            'schedule': parse_schedule(tasks.iam_sync.schedule),
            'options': _options(tasks.iam_sync.schedule),
        }
    if tasks.reap is not None and tasks.reap.schedule is not None:
        sched['reap-offboarded'] = {
            'task': 'cheeto.reap',
            'schedule': parse_schedule(tasks.reap.schedule),
            'options': _options(tasks.reap.schedule),
        }
    if tasks.ldap_sync is not None and tasks.ldap_sync.schedule is not None:
        for site in (tasks.ldap_sync.sites or daemon.sites):
            sched[f'ldap-sync-{site}'] = {
                'task': 'cheeto.ldap_sync',
                'args': [site],
                'schedule': parse_schedule(tasks.ldap_sync.schedule),
                'options': _options(tasks.ldap_sync.schedule),
            }
    if tasks.slurm_sync is not None and tasks.slurm_sync.schedule is not None:
        for site in (tasks.slurm_sync.sites or daemon.sites):
            sched[f'slurm-sync-{site}'] = {
                'task': 'cheeto.slurm_sync',
                'args': [site],
                'schedule': parse_schedule(tasks.slurm_sync.schedule),
                'options': _options(tasks.slurm_sync.schedule,
                                    queue=f'slurm.{site}'),
            }
    if tasks.sympa is not None and tasks.sympa.schedule is not None:
        for site in (tasks.sympa.sites or daemon.sites):
            sched[f'sympa-export-{site}'] = {
                'task': 'cheeto.sympa_export',
                'args': [site],
                'schedule': parse_schedule(tasks.sympa.schedule),
                'options': _options(tasks.sympa.schedule),
            }
    return sched
