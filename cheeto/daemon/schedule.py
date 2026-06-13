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


# Task shorthand → (registered task name, takes a sitename arg). The
# shorthands double as the `cheeto daemon enqueue` CLI vocabulary and match
# the daemon.tasks config keys (with `hippo` spelled out).
TASK_SPECS: dict[str, tuple[str, bool]] = {
    'hippo_process': ('cheeto.hippo_process', False),
    'iam_sync': ('cheeto.iam_sync', False),
    'reap': ('cheeto.reap', False),
    'ldap_sync': ('cheeto.ldap_sync', True),
    'slurm_sync': ('cheeto.slurm_sync', True),
    'sympa_export': ('cheeto.sympa_export', True),
    'puppet_sync': ('cheeto.puppet_sync', True),
}


def _configured_sites(config: Config, task: str) -> list[str]:
    """Per-site default fan-out for `task`: the task config's `sites`
    override when set, else the daemon-wide site list — the same resolution
    `build_beat_schedule` uses."""
    tcfg = getattr(config.daemon.tasks, 'sympa' if task == 'sympa_export'
                   else task, None)
    if tcfg is not None and getattr(tcfg, 'sites', None):
        return list(tcfg.sites)
    return list(config.daemon.sites)


def build_enqueue_entries(
    config: Config,
    task: str,
    sites: list[str] | None = None,
) -> list[dict]:
    """One-off submissions for `task`: entries of
    `{'task': <name>, 'args': [...], 'options': {'queue': ...}}` mirroring
    the beat routing (slurm_sync → `slurm.<site>`, everything else → the
    default `cheeto` queue), but with no expiry — an explicitly enqueued
    task should run even if the worker picks it up late.

    Per-site tasks fan out over `sites` (or the configured sites when not
    given); singleton tasks reject a sites argument.
    """
    if task not in TASK_SPECS:
        raise ValueError(
            f'Unknown task {task!r}; expected one of {sorted(TASK_SPECS)}'
        )
    name, per_site = TASK_SPECS[task]
    if not per_site:
        if sites:
            raise ValueError(f'Task {task!r} does not take a site')
        return [{'task': name, 'args': [], 'options': {'queue': 'cheeto'}}]

    targets = sites or _configured_sites(config, task)
    if not targets:
        raise ValueError(
            f'Task {task!r} is per-site but no sites were given or configured'
        )
    return [
        {
            'task': name,
            'args': [site],
            'options': {
                'queue': (
                    f'slurm.{site}' if task == 'slurm_sync' else 'cheeto'
                ),
            },
        }
        for site in targets
    ]


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
    if tasks.puppet_sync is not None and tasks.puppet_sync.schedule is not None:
        # Default `cheeto` queue: the repo clone lives on the hub host.
        for site in (tasks.puppet_sync.sites or daemon.sites):
            sched[f'puppet-sync-{site}'] = {
                'task': 'cheeto.puppet_sync',
                'args': [site],
                'schedule': parse_schedule(tasks.puppet_sync.schedule),
                'options': _options(tasks.puppet_sync.schedule),
            }
    return sched
