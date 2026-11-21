from argparse import Namespace
from datetime import timezone

from dateutil import parser as dateparser
from ponderosa import ArgParser
from rich.table import Table

from .. import commands
from . import parent
from ...constants import API_SCOPE_ALL, API_SCOPES
from ...log import Console
from ...operations import CreateApiKey, RevokeApiKey
from ...queries import list_api_keys
from ...yaml import print_yaml
from ._args import run_per_target, yaml_args


parent('ng', 'apikey', help='REST API keys: issue, list, revoke')


def _parse_expiry(value: str):
    """A date or datetime; a naive value is taken as UTC."""
    when = dateparser.parse(value)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when


def _when(value) -> str:
    return value.strftime('%Y-%m-%d %H:%M') if value is not None else '—'


def _key_to_dict(key) -> dict:
    """Never the hash: this is what `list` shows."""
    return {
        'name': key.name,
        'key_id': key.key_id,
        'scopes': list(key.scopes),
        'state': key.state(),
        'created_at': key.created_at,
        'expires_at': key.expires_at,
        'last_used_at': key.last_used_at,
        'revoked_at': key.revoked_at,
        'comment': key.comment or None,
    }


# ---------------------------------------------------------------------------
# `ng apikey new`
# ---------------------------------------------------------------------------


@commands.register('ng', 'apikey', 'new',
                   help='Issue an API key; the key is shown one time')
async def apikey_new(args: Namespace):
    console = Console()
    try:
        key, token = await CreateApiKey.run(
            args.db, args.author,
            name=args.name, scopes=args.scope,
            expires_at=args.expires, comment=args.comment,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1

    if args.yaml:
        print_yaml({**_key_to_dict(key), 'key': token})
        return
    console.print(
        f'Issued API key [green]{key.name}[/] '
        f'(id [cyan]{key.key_id}[/], scopes: {" ".join(key.scopes)})'
    )
    console.print('Send it in the [bold]X-API-Key[/] header:')
    # Plain print: no markup, no wrapping, so the key copies cleanly.
    print(f'\n    {token}\n')
    console.print(
        '[yellow]Store it now.[/] cheeto keeps only a hash and cannot '
        'show the key again.'
    )


@apikey_new.args()
def _(parser: ArgParser):
    parser.add_argument('name',
                        help='Label for the client that gets the key, for '
                             'example "hippo". Not unique: issue a second '
                             'key with the same name to rotate.')
    parser.add_argument('--scope', '-s', action='append', required=True,
                        choices=[*API_SCOPES, API_SCOPE_ALL],
                        help='A scope the key holds; repeat for more. '
                             f'{API_SCOPE_ALL} is every scope.')
    parser.add_argument('--expires', type=_parse_expiry, default=None,
                        metavar='DATE',
                        help='Date or datetime (UTC) after which the key '
                             'stops working (default: never)')
    parser.add_argument('--comment', default='',
                        help='Note for operators, for example who holds '
                             'the key')
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML to stdout (includes the key)')


# ---------------------------------------------------------------------------
# `ng apikey list`
# ---------------------------------------------------------------------------


@yaml_args.apply()
@commands.register('ng', 'apikey', 'list',
                   help='List API keys (never shows a key itself)')
async def apikey_list(args: Namespace):
    console = Console()
    keys = await list_api_keys(include_revoked=args.all)
    if args.yaml:
        print_yaml([_key_to_dict(k) for k in keys])
        return

    table = Table(title=f'API keys (count={len(keys)})')
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('key id', style='cyan', no_wrap=True)
    table.add_column('scopes')
    table.add_column('state')
    table.add_column('created')
    table.add_column('expires')
    table.add_column('last used')
    table.add_column('comment')
    state_style = {'active': 'green', 'expired': 'yellow', 'revoked': 'red'}
    for k in keys:
        state = k.state()
        table.add_row(
            k.name, k.key_id, ' '.join(k.scopes),
            f'[{state_style[state]}]{state}[/]',
            _when(k.created_at), _when(k.expires_at), _when(k.last_used_at),
            k.comment,
        )
    console.print(table)


@apikey_list.args()
def _(parser: ArgParser):
    parser.add_argument('--all', '-a', action='store_true', default=False,
                        help='Include revoked keys')


# ---------------------------------------------------------------------------
# `ng apikey revoke`
# ---------------------------------------------------------------------------


@commands.register('ng', 'apikey', 'revoke',
                   help='Revoke API keys by key id; effective at once')
async def apikey_revoke(args: Namespace):
    console = Console()

    async def revoke(key_id: str):
        await RevokeApiKey.run(args.db, args.author, key_id=key_id)

    console.print('Revoking API key(s):')
    return await run_per_target(console, args.key_id, revoke, ok='revoked')


@apikey_revoke.args()
def _(parser: ArgParser):
    parser.add_argument('key_id', nargs='+',
                        help='The key id(s), from `ng apikey list`')
