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

from ...operations.base import UNSET


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
def user_args(parser: ArgParser, required: bool = False,
              multiple: bool = False):
    if multiple:
        parser.add_argument('--user', '-u', nargs='+', default=None,
                            required=required, metavar='USER',
                            help='Username(s)')
    else:
        parser.add_argument('--user', '-u', default=None, required=required,
                            help='Username')


@arggroup('group')
def group_args(parser: ArgParser, required: bool = False,
               multiple: bool = False):
    if multiple:
        parser.add_argument('--group', '-g', nargs='+', default=None,
                            required=required, metavar='GROUP',
                            help='Group name(s)')
    else:
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


def template_arg(raw: str) -> tuple[str, str]:
    """argparse type for `--zfs-path-template CATEGORY=TEMPLATE` tokens."""
    category, sep, template = raw.partition('=')
    if not sep or not category.strip() or not template.strip():
        raise argparse.ArgumentTypeError(
            f'{raw!r}: expected CATEGORY=TEMPLATE, e.g. '
            "home=/{host}/home/{name}"
        )
    return category.strip(), template.strip()


@arggroup('storage_defaults')
def storage_defaults_args(parser: ArgParser, scope: str = 'site',
                          clearable: bool = True):
    """Flags for a storage-defaults tier (site or storage host): NFS export
    options/ranges and per-category ZFS path templates. `clearable=False`
    (create commands) omits the --clear-* flags."""
    parser.add_argument('--export-options', default=None, metavar='OPTIONS',
                        help=f'Default NFS export options for the {scope} '
                             '(e.g. rw,no_root_squash,sync,no_subtree_check)')
    parser.add_argument('--export-ranges', nargs='+', default=None,
                        metavar='RANGE',
                        help=f'Default NFS export client ranges for the '
                             f'{scope} (CIDRs or hosts); replaces the list')
    parser.add_argument('--zfs-path-template', action='append', default=None,
                        type=template_arg, metavar='CATEGORY=TEMPLATE',
                        help="ZFS path template for a storage category, e.g. "
                             "home='/{host}/home/{name}' (fields: {host}, "
                             "{name}, {site}); repeatable")
    if clearable:
        parser.add_argument('--clear-export', action='store_true',
                            default=False,
                            help=f'Remove the {scope}-level export config so '
                                 'the next tier applies')
        parser.add_argument('--clear-zfs-path-template', action='append',
                            default=None, metavar='CATEGORY',
                            help='Remove the ZFS path template for a category; '
                                 'repeatable')


def has_storage_defaults_args(args) -> bool:
    return any((
        args.export_options is not None,
        args.export_ranges is not None,
        args.zfs_path_template,
        getattr(args, 'clear_export', False),
        getattr(args, 'clear_zfs_path_template', None),
    ))


def storage_defaults_kwargs(args) -> dict:
    """Operation kwargs from the `storage_defaults_args` flags: argparse None
    -> UNSET (leave alone); the --clear-* flags map to the clear kwargs."""
    return {
        'export_options': (
            UNSET if args.export_options is None else args.export_options
        ),
        'export_ranges': (
            UNSET if args.export_ranges is None else args.export_ranges
        ),
        'clear_nfs_export': getattr(args, 'clear_export', False),
        'zfs_path_templates': dict(args.zfs_path_template or []) or None,
        'clear_zfs_path_templates': (
            list(getattr(args, 'clear_zfs_path_template', None) or []) or None
        ),
    }


def confirm_typed(console, kind: str, name: str, force: bool = False) -> bool:
    """Typed-name confirmation for destructive commands: the operator must
    re-type the object's exact name. `force=True` skips the prompt.
    Returns False (after printing the abort) on mismatch or EOF/interrupt."""
    if force:
        return True
    console.print(
        f'[bold red]DANGER:[/] this permanently deletes {kind} '
        f'[bold]{name}[/] and its references.'
    )
    try:
        answer = input(f"Type the {kind}'s name to confirm: ").strip()
    except (EOFError, KeyboardInterrupt):
        console.print('\n[yellow]Aborted[/]')
        return False
    if answer != name:
        console.print('[yellow]Name mismatch — aborted[/]')
        return False
    return True


async def run_per_target(console, targets, fn, *,
                         ok: str = 'done') -> int:
    """Drive a multi-target command: `await fn(target)` for each target,
    continuing past per-target ValueErrors. Each Operation.run() is its
    own transaction + History row, so one target's failure never affects
    another's write. Prints one line per target and a failure summary;
    returns 1 if any target failed, else 0."""
    failed: list[str] = []
    for target in targets:
        try:
            await fn(target)
        except ValueError as e:
            console.print(f'  [red]{target}: {e}[/]')
            failed.append(target)
        else:
            console.print(f'  [green]{target}[/]: {ok}')
    if failed:
        console.print(
            f'[red]{len(failed)}/{len(targets)} failed:[/] '
            f'{", ".join(failed)}'
        )
        return 1
    return 0
