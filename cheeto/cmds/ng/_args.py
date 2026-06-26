"""Shared argparse argument groups for the `cheeto ng` command tree.

Each arggroup adds a single flag argument to a parser so commands can
compose them via decorator stacking, matching the convention established
by the old `cheeto db` commands.
"""

import argparse
import re
from datetime import datetime, timedelta, timezone

from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta
from ponderosa import ArgParser, arggroup


# Sentinel returned by expirable_value when the user asks to unset the field.
# Keeping it as a string makes the CLI value reflect-able into describe() logs
# and easy to spot at call sites.
EXPIRABLE_CLEAR = 'CLEAR'

_DURATION_RE = re.compile(
    r'^\+\s*(?P<n>\d+)\s*(?P<unit>s|min|m|h|d|w|mo|y)$',
    re.IGNORECASE,
)
# 'm' is ambiguous (minute vs month). We map bare 'm' to minute since users
# typing month conventionally write 'mo'; the regex above accepts both.
_UNIT_TO_DELTA = {
    's': lambda n: timedelta(seconds=n),
    'min': lambda n: timedelta(minutes=n),
    'm': lambda n: timedelta(minutes=n),
    'h': lambda n: timedelta(hours=n),
    'd': lambda n: timedelta(days=n),
    'w': lambda n: timedelta(weeks=n),
    'mo': lambda n: relativedelta(months=n),
    'y': lambda n: relativedelta(years=n),
}


def expirable_value(raw: str) -> datetime | str:
    """argparse type for --expires-at / --provisioned-at style flags.

    Accepts:
      - ISO 8601 timestamp ('2027-01-15', '2027-01-15T12:00:00Z', etc.)
      - Relative duration from 'now', e.g. '+30d', '+6mo', '+1y', '+2w'
      - The literal 'clear' / 'none' / 'null' to unset (returns EXPIRABLE_CLEAR)

    Returned datetimes are timezone-aware (UTC). The CLEAR sentinel is a
    string so it survives translation into operation describe() payloads.
    """
    if raw is None:
        return None  # type: ignore[return-value]
    s = raw.strip()
    if s.lower() in {'clear', 'none', 'null', ''}:
        return EXPIRABLE_CLEAR

    m = _DURATION_RE.match(s)
    if m:
        n = int(m.group('n'))
        unit = m.group('unit').lower()
        delta = _UNIT_TO_DELTA[unit](n)
        return datetime.now(timezone.utc) + delta

    try:
        dt = isoparse(s)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f'{raw!r} is not a recognized timestamp. Use an ISO 8601 '
            "datetime ('2027-01-15'), a relative duration ('+30d', '+6mo', "
            "'+1y'), or 'clear' to unset."
        ) from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@arggroup('expirable')
def expirable_args(parser: ArgParser, scope: str = 'record'):
    """Add --expires-at and --provisioned-at flags.

    `scope` is a short noun used in help text (e.g. 'allocation', 'user').
    Each flag accepts an ISO 8601 timestamp, a relative duration like
    '+30d' / '+6mo' / '+1y', or 'clear' to unset.
    """
    parser.add_argument(
        '--expires-at', default=None, type=expirable_value,
        metavar='WHEN',
        help=f"Set the {scope}'s expiration timestamp. Accepts ISO 8601 "
             "(e.g. '2027-01-15'), a relative duration from now "
             "('+30d', '+6mo', '+1y'), or 'clear' to unset.",
    )
    parser.add_argument(
        '--provisioned-at', default=None, type=expirable_value,
        metavar='WHEN',
        help=f"Set the {scope}'s provisioning timestamp. Same value formats "
             "as --expires-at.",
    )


@arggroup('site')
def site_args(parser: ArgParser, required: bool = False):
    parser.add_argument('--site', '-s', default=None, required=required,
                        help='Site name')


@arggroup('user')
def user_args(parser: ArgParser, required: bool = False):
    parser.add_argument('--user', '-u', default=None, required=required,
                        help='Username')


@arggroup('group')
def group_args(parser: ArgParser, required: bool = False):
    parser.add_argument('--group', '-g', default=None, required=required,
                        help='Group name')


@arggroup('email')
def email_args(parser: ArgParser,
               required: bool = True,
               default: str | None = None):
    if default is not None:
        parser.add_argument('--email', default=default,
                            help='Email address')
    else:
        parser.add_argument('--email', required=required, default=None,
                            help='Email address')


@arggroup('fullname')
def fullname_args(parser: ArgParser, required: bool = True):
    parser.add_argument('--fullname', default=None, required=required,
                        help='Full display name')


@arggroup('password')
def password_args(parser: ArgParser):
    parser.add_argument('--password', action='store_true', default=False,
                        help='Generate a random password for the user')


@arggroup('yaml')
def yaml_args(parser: ArgParser):
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML to stdout')
