from argparse import Namespace
from datetime import timezone

from dateutil import parser as dateparser
from ponderosa import ArgParser
from rich.table import Table

from .. import commands
from ...log import Console
from ...models.user import User
from ...operations import operation_names
from ...queries import find_history
from ...yaml import print_yaml
from ._args import user_args, yaml_args


def _parse_dt(value: str):
    """Parse a date or datetime string; a naive value is taken as UTC to match
    how History timestamps are stored and displayed."""
    dt = dateparser.parse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@user_args.apply()
@yaml_args.apply()
@commands.register('ng', 'history',
                   help='Query operation history')
async def history_cmd(args: Namespace):
    console = Console()

    author_id = None
    if args.user:
        author = await User.find_one(User.name == args.user)
        if author is None:
            console.print(f'[red]User {args.user} not found[/]')
            return 1
        author_id = author.id

    since = until = None
    try:
        if args.since:
            since = _parse_dt(args.since)
        if args.until:
            until = _parse_dt(args.until)
    except (ValueError, OverflowError) as e:
        console.print(f'[red]Could not parse date/time: {e}[/]')
        return 1

    entries = await find_history(
        op=args.op, author_id=author_id,
        since=since, until=until, limit=args.limit,
    )

    if args.yaml:
        rows = []
        for entry in entries:
            if isinstance(entry.author, User):
                author = entry.author.name
            elif entry.author is None:
                author = None
            else:
                author = 'Unknown'
            rows.append({
                'timestamp': entry.timestamp,
                'op': entry.op,
                'author': author,
                'changes': entry.changes,
            })
        print_yaml(rows)
        return 0

    if not entries:
        console.print('[dim]No history entries found[/]')
        return 0

    table = Table(title='Operation History')
    table.add_column('Timestamp', style='cyan')
    table.add_column('Operation', style='green')
    table.add_column('Author', style='yellow')
    table.add_column('Changes')

    for entry in entries:
        # entry.author is a resolved User when the link still points at a
        # live record, None when it was never set, or an unresolved Link
        # when the target was dropped (e.g. collections wiped) — fetch_links
        # leaves the raw Link in place rather than raising.
        if isinstance(entry.author, User):
            author_name = entry.author.name
        elif entry.author is None:
            author_name = ''
        else:
            author_name = 'Unknown'
        ts = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        changes = ', '.join(f'{k}={v}' for k, v in entry.changes.items())
        table.add_row(ts, entry.op, author_name, changes)

    console.print(table)


@history_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--op', default=None, choices=sorted(operation_names()),
                        help='Filter by operation name (e.g., create_user)')
    parser.add_argument('--since', default=None,
                        help='Only entries at/after this date or datetime '
                             '(e.g. 2026-06-01 or "2026-06-01 14:00"); '
                             'naive values are UTC')
    parser.add_argument('--until', default=None,
                        help='Only entries at/before this date or datetime '
                             '(inclusive; naive values are UTC)')
    parser.add_argument('--limit', '-n', type=int, default=50,
                        help='Maximum entries to show (default: 50; 0 for unlimited)')
