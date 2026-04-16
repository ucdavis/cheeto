from argparse import Namespace

from ponderosa import ArgParser
from rich.table import Table

from .. import commands
from ...log import Console
from ...models.history import History
from ...models.user import User
from ._args import user_args


@user_args.apply()
@commands.register('ng', 'history',
                   help='Query operation history')
async def history_cmd(args: Namespace):
    console = Console()

    filters = []
    if args.op:
        filters.append(History.op == args.op)
    if args.user:
        author = await User.find_one(User.name == args.user)
        if author is None:
            console.print(f'[red]User {args.user} not found[/]')
            return 1
        filters.append(History.author.id == author.id)

    query = History.find(*filters).sort('-timestamp').limit(args.limit)
    entries = await query.to_list()

    if not entries:
        console.print('[dim]No history entries found[/]')
        return 0

    table = Table(title='Operation History')
    table.add_column('Timestamp', style='cyan')
    table.add_column('Operation', style='green')
    table.add_column('Author', style='yellow')
    table.add_column('Changes')

    for entry in entries:
        author_name = ''
        if entry.author is not None:
            if isinstance(entry.author, User):
                author_name = entry.author.name
            else:
                await entry.fetch_link(History.author)
                author_name = entry.author.name if entry.author else '?'

        ts = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        changes = ', '.join(f'{k}={v}' for k, v in entry.changes.items())
        table.add_row(ts, entry.op, author_name, changes)

    console.print(table)


@history_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--op', default=None,
                        help='Filter by operation name (e.g., create_user)')
    parser.add_argument('--limit', '-n', type=int, default=50,
                        help='Maximum entries to show (default: 50)')
