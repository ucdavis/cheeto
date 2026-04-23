from argparse import Namespace

from ponderosa import ArgParser
from rich.table import Table

from .. import commands
from ...constants import HIPPO_EVENT_ACTIONS, HIPPO_EVENT_STATUSES
from ...hippoapi.api.action import action_sync_puppet_accounts
from ...hippoapi.api.event_queue import event_queue_pending_events
from ...log import Console
from ...models.group import Group
from ...models.hippo import HippoEvent
from ...models.site import Site
from ...models.user import User
from ...operations.hippo import (
    HippoEventProcessor,
    filter_events,
    hippoapi_client,
)
from ...yaml import dumps as dumps_yaml, highlight_yaml
from ._args import site_args, user_args


@commands.register('ng', 'hippo',
                   help='Interaction with the HiPPO API (async/beanie)')
def hippo_cmd(args: Namespace):
    pass


@commands.register('ng', 'hippo', 'process',
                   help='Process pending events from the HiPPO API')
async def hippo_process(args: Namespace):
    console = Console()
    processor = HippoEventProcessor(
        args.db, args.config.hippo, author=args.author,
    )
    await processor.process(
        post_back=args.post,
        event_type=args.type,
        event_id=args.id,
    )
    console.print('[green]Done processing events[/]')


@hippo_process.args()
def _(parser: ArgParser):
    parser.add_argument('--post', action='store_true', default=False,
                        help='Post completion status back to HiPPO')
    parser.add_argument('--id', type=int, default=None,
                        help='Process only this event id')
    parser.add_argument('--type', choices=list(HIPPO_EVENT_ACTIONS),
                        default=None,
                        help='Process only events of this action')


@commands.register('ng', 'hippo', 'events',
                   help='List pending events from the HiPPO API (upstream)')
async def hippo_events(args: Namespace):
    console = Console()
    with hippoapi_client(args.config.hippo, quiet=args.quiet) as client:
        events = await event_queue_pending_events.asyncio(client=client) or []
    for event in filter_events(events, args.type, args.id):
        console.print(event.to_dict() if hasattr(event, 'to_dict') else event)


@hippo_events.args()
def _(parser: ArgParser):
    parser.add_argument('--id', type=int, default=None,
                        help='Show only this event id')
    parser.add_argument('--type', choices=list(HIPPO_EVENT_ACTIONS),
                        default=None,
                        help='Show only events of this action')


@site_args.apply()
@user_args.apply()
@commands.register('ng', 'hippo', 'list',
                   help='Query locally-stored HippoEvent records')
async def hippo_list(args: Namespace):
    console = Console()
    filters = []
    if args.status:
        filters.append(HippoEvent.status == args.status)
    if args.action:
        filters.append(HippoEvent.action == args.action)
    if args.user:
        user = await User.find_one(User.name == args.user)
        if user is None:
            console.print(f'[red]User {args.user} not found[/]')
            return 1
        filters.append(HippoEvent.target_user.id == user.id)
    if args.site:
        site = await Site.find_one(Site.name == args.site)
        if site is None:
            console.print(f'[red]Site {args.site} not found[/]')
            return 1
        filters.append(HippoEvent.site.id == site.id)

    events = await (
        HippoEvent.find(*filters)
        .sort('-first_seen_at')
        .limit(args.limit)
        .to_list()
    )
    if not events:
        console.print('[dim](no events match)[/]')
        return 0

    table = Table(title='HippoEvents')
    table.add_column('hippo_id', style='cyan')
    table.add_column('action', style='green')
    table.add_column('status', style='yellow')
    table.add_column('site')
    table.add_column('target', style='magenta')
    table.add_column('groups', style='dim')
    table.add_column('tries')
    table.add_column('first_seen', style='dim')

    for ev in events:
        site_name = ev.site.name if ev.site and hasattr(ev.site, 'name') else ''
        target = ev.target_username or ''
        groups = ', '.join(ev.target_groupnames)
        ts = ev.first_seen_at.strftime('%Y-%m-%d %H:%M') if ev.first_seen_at else ''
        table.add_row(
            str(ev.hippo_id), ev.action, ev.status, site_name,
            target, groups, str(ev.n_tries), ts,
        )
    console.print(table)


@hippo_list.args()
def _(parser: ArgParser):
    parser.add_argument('--status', choices=list(HIPPO_EVENT_STATUSES),
                        default=None,
                        help='Filter by status')
    parser.add_argument('--action', choices=list(HIPPO_EVENT_ACTIONS),
                        default=None,
                        help='Filter by action')
    parser.add_argument('--limit', '-n', type=int, default=50,
                        help='Maximum entries to show (default: 50)')


@commands.register('ng', 'hippo', 'show',
                   help='Show a single locally-stored HippoEvent')
async def hippo_show(args: Namespace):
    console = Console()
    filters = [HippoEvent.hippo_id == args.id]
    if args.endpoint:
        filters.append(HippoEvent.hippo_endpoint == args.endpoint)
    event = await HippoEvent.find_one(*filters, fetch_links=True)
    if event is None:
        console.print(f'[red]HippoEvent id={args.id} not found[/]')
        return 1

    data = {
        'hippo_id': event.hippo_id,
        'hippo_endpoint': event.hippo_endpoint,
        'action': event.action,
        'status': event.status,
        'n_tries': event.n_tries,
        'cluster': event.cluster,
        'site': event.site.name if event.site else None,
        'target_username': event.target_username,
        'target_groupnames': event.target_groupnames,
        'sponsor_username': event.sponsor_username,
        'queued_at': event.queued_at,
        'first_seen_at': event.first_seen_at,
        'completed_at': event.completed_at,
        'last_error': event.last_error,
        'raw': event.raw,
    }
    console.print(highlight_yaml(dumps_yaml(data)))


@hippo_show.args()
def _(parser: ArgParser):
    parser.add_argument('--id', type=int, required=True,
                        help='HiPPO event id')
    parser.add_argument('--endpoint', default=None,
                        help='Optional endpoint filter (disambiguates events '
                             'across environments)')


@commands.register('ng', 'hippo', 'sync-puppet',
                   help='Force a HiPPO sync from puppet YAML files')
async def hippo_sync_puppet(args: Namespace):
    console = Console()
    with hippoapi_client(args.config.hippo, quiet=args.quiet) as client:
        resp = await action_sync_puppet_accounts.asyncio_detailed(client=client)
    console.print(f'status: {resp.status_code}')
    if resp.content:
        console.print(resp.content)
