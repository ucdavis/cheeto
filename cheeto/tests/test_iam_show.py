"""Unit tests for the IAM offboarding display helpers behind `ng user show`
and `ng iam show` (`cheeto/cmds/ng/_iam_show.py`). Pure: no DB, no fixtures.

The phase table mirrors `cheeto.operations.iam`: missing -> in grace ->
offboarding (expires_at set) -> inactive (reaped), plus the edge phases the
sync and reaper leave behind."""

import io
import re
from datetime import datetime, timedelta, timezone

import pytest
from rich.console import Console
from rich.table import Table

from cheeto.cmds.ng._iam_show import (
    add_offboarding_rows,
    fmt_remaining,
    iam_offboarding,
)
from cheeto.models.user import UCDIAMInfo, UCDIAMPerson
from cheeto.yaml import dumps

NOW = datetime(2026, 10, 20, 21, 3, 11)
GRACE = 3
OFFSET = 30
PERSON = UCDIAMPerson(iam_id=1000000001, mothra_id=1000001)

# Dates from the approved previews.
FMA_IN_GRACE = datetime(2026, 10, 19, 9, 5, 0, 146000)
FMA_OFFBOARDING = datetime(2026, 10, 1, 9, 5, 0, 146000)
EXPIRES_OFFBOARDING = datetime(2026, 11, 3, 9, 5, 0, 146000)
FMA_INACTIVE = datetime(2026, 6, 19, 20, 43, 55, 109000)
EXPIRES_INACTIVE = datetime(2026, 7, 22, 20, 43, 55, 109000)


def _missing(first_missing_at) -> UCDIAMInfo:
    return UCDIAMInfo(iam_status='missing', person=PERSON,
                      iam_synced_at=NOW, first_missing_at=first_missing_at)


def _present() -> UCDIAMInfo:
    return UCDIAMInfo(iam_status='present', person=PERSON,
                      iam_synced_at=NOW, last_seen_at=NOW)


def _phase(iam, status, expires_at=None, now=NOW):
    return iam_offboarding(
        iam, status=status, expires_at=expires_at,
        grace_days=GRACE, expiry_offset_days=OFFSET, now=now,
    )


def _render(offboarding) -> str:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column()
    table.add_column()
    add_offboarding_rows(table, offboarding)
    # Wide console so no value wraps; collapse the column padding so a
    # 'label value' substring assert reads naturally.
    console = Console(record=True, width=400, file=io.StringIO())
    console.print(table)
    return re.sub(r' {2,}', ' ', console.export_text())


IN_GRACE = dict(iam=_missing(FMA_IN_GRACE), status='active')
GRACE_ENDED = dict(iam=_missing(NOW - timedelta(days=5)), status='active')
EXPIRING = dict(iam=_missing(NOW - timedelta(days=5)), status='active',
                expires_at=NOW + timedelta(days=10))
OFFBOARDING = dict(iam=_missing(FMA_OFFBOARDING), status='offboarding',
                   expires_at=EXPIRES_OFFBOARDING)
REAP_PENDING = dict(iam=_missing(FMA_OFFBOARDING), status='offboarding',
                    expires_at=NOW - timedelta(hours=1))
INACTIVE = dict(iam=_missing(FMA_INACTIVE), status='inactive',
                expires_at=EXPIRES_INACTIVE)
DISABLED = dict(iam=_missing(NOW - timedelta(days=5)), status='disabled')
RESTORING = dict(iam=_present(), status='offboarding',
                 expires_at=NOW + timedelta(days=10))
RESTORABLE = dict(iam=_present(), status='inactive',
                  expires_at=EXPIRES_INACTIVE)
MISSING_NO_FMA = dict(iam=UCDIAMInfo(iam_status='missing'), status='active')

ALL_PHASES = {
    'in_grace': IN_GRACE,
    'grace_ended': GRACE_ENDED,
    'expiring': EXPIRING,
    'offboarding': OFFBOARDING,
    'reap_pending': REAP_PENDING,
    'inactive': INACTIVE,
    'disabled': DISABLED,
    'restoring': RESTORING,
    'restorable': RESTORABLE,
    'missing': MISSING_NO_FMA,
}


class TestFmtRemaining:

    @pytest.mark.parametrize('td, expected', [
        (timedelta(0), '0d 0h'),
        (timedelta(hours=-3), '0d 0h'),
        (timedelta(minutes=59), '0d 0h'),
        (timedelta(days=1, hours=12, minutes=1, seconds=49), '1d 12h'),
        (timedelta(days=13, hours=12), '13d 12h'),
        (timedelta(days=40), '40d 0h'),
    ])
    def test_format(self, td, expected):
        assert fmt_remaining(td) == expected


class TestIamOffboarding:

    def test_no_iam_data(self):
        assert _phase(None, 'active') is None

    def test_present_active_is_quiet(self):
        assert _phase(_present(), 'active') is None

    def test_present_disabled_is_quiet(self):
        # Operator-disabled and present in IAM: the pipeline never touches it.
        assert _phase(_present(), 'disabled') is None

    def test_in_grace(self):
        ob = _phase(**IN_GRACE)
        grace_ends = FMA_IN_GRACE + timedelta(days=GRACE)
        assert ob == {
            'phase': 'in_grace',
            'grace_ends_at': grace_ends,
            'grace_remaining': '1d 12h',
            'projected_expires_at': grace_ends + timedelta(days=OFFSET),
        }

    def test_in_grace_aware_first_missing_at_is_normalized(self):
        # Some test datasets store aware datetimes; prod stores naive UTC.
        aware = FMA_IN_GRACE.replace(tzinfo=timezone.utc)
        ob = _phase(_missing(aware), 'active')
        assert ob['phase'] == 'in_grace'
        assert ob['grace_ends_at'].tzinfo is None
        assert ob['grace_ends_at'] == FMA_IN_GRACE + timedelta(days=GRACE)

    def test_grace_ended_but_sync_has_not_flipped(self):
        ob = _phase(**GRACE_ENDED)
        grace_ends = NOW - timedelta(days=2)
        assert ob == {
            'phase': 'grace_ended',
            'grace_ended_at': grace_ends,
            'projected_expires_at': grace_ends + timedelta(days=OFFSET),
        }

    def test_expiring_after_grace(self):
        ob = _phase(**EXPIRING)
        assert ob == {
            'phase': 'expiring',
            'expires_at': NOW + timedelta(days=10),
            'grace_ended_at': NOW - timedelta(days=2),
        }

    def test_expiring_in_grace(self):
        ob = _phase(_missing(NOW - timedelta(days=1)), 'active',
                    expires_at=NOW + timedelta(days=10))
        assert ob['phase'] == 'expiring'
        assert ob['grace_ends_at'] == NOW + timedelta(days=2)
        assert ob['grace_remaining'] == '2d 0h'
        assert 'grace_ended_at' not in ob

    def test_offboarding(self):
        ob = _phase(**OFFBOARDING)
        assert ob == {
            'phase': 'offboarding',
            'grace_ended_at': FMA_OFFBOARDING + timedelta(days=GRACE),
            'deactivates_at': EXPIRES_OFFBOARDING,
            'deactivates_in': '13d 12h',
            'reap_pending': False,
        }

    def test_offboarding_reap_pending(self):
        ob = _phase(**REAP_PENDING)
        assert ob['phase'] == 'offboarding'
        assert ob['reap_pending'] is True
        assert ob['deactivates_in'] is None
        assert ob['deactivates_at'] == NOW - timedelta(hours=1)

    def test_offboarding_without_expires_at(self):
        ob = _phase(_missing(FMA_OFFBOARDING), 'offboarding')
        assert ob['phase'] == 'offboarding'
        assert ob['deactivates_at'] is None
        assert ob['deactivates_in'] is None
        assert ob['reap_pending'] is False

    def test_offboarding_aware_expires_at_is_normalized(self):
        aware = EXPIRES_OFFBOARDING.replace(tzinfo=timezone.utc)
        ob = _phase(_missing(FMA_OFFBOARDING), 'offboarding', expires_at=aware)
        assert ob['deactivates_at'] == EXPIRES_OFFBOARDING
        assert ob['deactivates_at'].tzinfo is None

    def test_inactive(self):
        ob = _phase(**INACTIVE)
        assert ob == {'phase': 'inactive', 'deactivated_at': EXPIRES_INACTIVE}

    def test_inactive_without_expires_at(self):
        ob = _phase(_missing(FMA_INACTIVE), 'inactive')
        assert ob == {'phase': 'inactive', 'deactivated_at': None}

    def test_disabled_after_grace(self):
        ob = _phase(**DISABLED)
        assert ob == {'phase': 'disabled',
                      'grace_ended_at': NOW - timedelta(days=2)}

    def test_disabled_in_grace(self):
        ob = _phase(_missing(NOW - timedelta(days=1)), 'disabled')
        assert ob == {'phase': 'disabled',
                      'grace_ends_at': NOW + timedelta(days=2)}

    def test_restoring(self):
        assert _phase(**RESTORING) == {'phase': 'restoring'}

    def test_restorable(self):
        assert _phase(**RESTORABLE) == {
            'phase': 'restorable', 'deactivated_at': EXPIRES_INACTIVE,
        }

    def test_missing_without_first_missing_at(self):
        assert _phase(**MISSING_NO_FMA) == {'phase': 'missing'}

    def test_unset_status_is_treated_as_active(self):
        ob = _phase(_missing(FMA_IN_GRACE), None)
        assert ob['phase'] == 'in_grace'

    def test_default_now(self):
        # now=None must work (the CLI never passes one).
        ob = iam_offboarding(
            _missing(datetime(2020, 1, 1)), status='inactive',
            expires_at=datetime(2020, 2, 1), grace_days=GRACE,
            expiry_offset_days=OFFSET,
        )
        assert ob['phase'] == 'inactive'


class TestAddOffboardingRows:

    def test_none_adds_nothing(self):
        table = Table()
        table.add_column()
        table.add_column()
        add_offboarding_rows(table, None)
        assert table.row_count == 0

    @pytest.mark.parametrize('name', sorted(ALL_PHASES))
    def test_every_phase_renders(self, name):
        text = _render(_phase(**ALL_PHASES[name]))
        assert 'offboarding' in text

    def test_in_grace_rows(self):
        text = _render(_phase(**IN_GRACE))
        assert 'in grace — 1d 12h remaining, still active' in text
        assert f'grace ends {FMA_IN_GRACE + timedelta(days=GRACE)}' in text
        assert 'projected expires_at 2026-11-21 09:05:00.146000' in text
        assert 'set by the first sync after grace ends' in text

    def test_grace_ended_rows(self):
        text = _render(_phase(**GRACE_ENDED))
        assert 'next sync sets expires_at and status offboarding' in text
        assert 'projected expires_at' in text
        assert 'or later' in text

    def test_offboarding_rows(self):
        text = _render(_phase(**OFFBOARDING))
        assert f'grace ended {FMA_OFFBOARDING + timedelta(days=GRACE)}' in text
        assert (f'deactivates {EXPIRES_OFFBOARDING} (expires_at) '
                f'— in 13d 12h') in text
        assert 'projected' not in text
        assert 'in grace' not in text
        assert 'next sync' not in text

    def test_offboarding_reap_pending_rows(self):
        text = _render(_phase(**REAP_PENDING))
        assert 'passed; next reap sets inactive' in text
        assert 'projected' not in text

    def test_inactive_rows(self):
        ob = _phase(**INACTIVE)
        text = _render(ob)
        assert f'complete — deactivated {EXPIRES_INACTIVE} (expires_at)' in text
        assert 'grace' not in text
        assert 'projected' not in text
        assert 'next sync' not in text
        assert len(text.strip().splitlines()) == 1

    def test_expiring_rows(self):
        text = _render(_phase(**EXPIRING))
        assert 'expires_at already set, sync leaves it' in text

    def test_disabled_rows(self):
        text = _render(_phase(**DISABLED))
        assert 'status disabled is operator-set' in text

    def test_restoring_rows(self):
        text = _render(_phase(**RESTORING))
        assert 'next sync restores active and clears expires_at' in text

    def test_restorable_rows(self):
        text = _render(_phase(**RESTORABLE))
        assert 'not auto-restored (see ng iam restorable)' in text
        assert str(EXPIRES_INACTIVE) in text

    def test_missing_rows(self):
        text = _render(_phase(**MISSING_NO_FMA))
        assert 'first_missing_at not recorded' in text


class TestYamlSafe:
    """The old projection put a raw timedelta into the --yaml dict, which the
    YAML dumper has no representer for. Every phase dict must dump."""

    @pytest.mark.parametrize('name', sorted(ALL_PHASES))
    def test_every_phase_dumps(self, name):
        text = dumps({'offboarding': _phase(**ALL_PHASES[name])})
        assert 'phase' in text

    def test_in_grace_dump_contents(self):
        text = dumps({'offboarding': _phase(**IN_GRACE)})
        assert 'in_grace' in text
        assert '1d 12h' in text
