import asyncio
import sys
from argparse import BooleanOptionalAction, Namespace
from pathlib import Path

import sh
from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from . import parent
from ...log import Console
from ...models.site import Site
from ...operations import (
    AddSiteAlias,
    AddStickyGroup,
    AddStickySlurmAccount,
    ClearSiteDefaultSlurmAccount,
    CreateSite,
    ExportPuppetStorage,
    ExportRootSSHKeys,
    ExportSympaEmails,
    DeleteSite,
    RemoveSiteAlias,
    RemoveStickyGroup,
    RemoveStickySlurmAccount,
    SetSiteDefaultSlurmAccount,
    SetSiteStorageDefaults,
    SyncOldPuppet,
)
from ...puppet import PuppetAccountMap
from ...queries import (
    count_site_dependents,
    find_site_by_name,
    resolve_group_names,
    resolve_site_storage_settings,
    resolve_slurm_account_label,
    resolve_slurm_account_labels,
    root_authorized_keys_text,
    site_to_puppet_legacy,
)
from ...yaml import dumps as dumps_yaml, highlight_yaml, print_yaml
from ._args import confirm_typed, group_args, run_per_target, site_args


# ---------------------------------------------------------------------------
# Grammar parents: every `ng site` verb namespace is registered before its
# leaves (ponderosa registrations execute top-to-bottom at import).
# ---------------------------------------------------------------------------


parent('ng', 'site', help='Site operations')
parent('ng', 'site', 'add',
       help='Attach aliases and sticky records to a site')
parent('ng', 'site', 'remove',
       help='Detach site sub-resources or delete a site', aliases=['rm'])
parent('ng', 'site', 'set', help='Set site-level properties')
parent('ng', 'site', 'sync', help='Site synchronization tasks')


@site_args.apply(required=True)
@commands.register('ng', 'site', 'new',
                   help='Create a new site')
async def site_new(args: Namespace):
    console = Console()
    site = await CreateSite.run(
        args.db, args.author,
        name=args.site, fqdn=args.fqdn,
    )
    console.print(f'Created site [green]{site.name}[/] ({site.fqdn})')


@site_new.args()
def _(parser: ArgParser):
    parser.add_argument('--fqdn', required=True)


# ---------------------------------------------------------------------------
# `ng site list` / `ng site remove site`
# ---------------------------------------------------------------------------


@commands.register('ng', 'site', 'list',
                   help='List all sites')
async def site_list(args: Namespace):
    console = Console()
    sites = await Site.find_all().sort('+name').to_list()
    if args.yaml:
        print_yaml([{'name': s.name, 'fqdn': s.fqdn} for s in sites])
        return
    table = Table(title=f'Sites (count={len(sites)})')
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('fqdn', style='cyan')
    for s in sites:
        table.add_row(s.name, s.fqdn)
    console.print(table)


@site_list.args()
def _(parser: ArgParser):
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')


@site_args.apply(required=True)
@commands.register('ng', 'site', 'delete',
                   help='Delete a site and all of its per-site records '
                        '(cascade)')
async def site_delete(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1

    counts = await count_site_dependents(site)
    table = Table(
        title=f'Records linked to [bold]{args.site}[/] (will be deleted)',
        show_header=False, box=None, pad_edge=False, padding=(0, 1),
    )
    table.add_column(style='cyan', no_wrap=True)
    table.add_column(justify='right')
    for label, n in counts.items():
        table.add_row(label, str(n))
    console.print(table)

    if not confirm_typed(console, 'site', args.site, force=args.force):
        return 1

    result = await DeleteSite.run(
        args.db, args.author, sitename=args.site, reason=args.reason,
    )
    deleted = sum(result.values())
    console.print(
        f'Deleted site [green]{args.site}[/] and {deleted} associated '
        f'record(s)'
    )


@site_delete.args()
def _(parser: ArgParser):
    parser.add_argument('--reason', required=True,
                        help='Why the site is being deleted (recorded in '
                             'History)')
    parser.add_argument('--force', '-f', action='store_true', default=False,
                        help='Skip the confirmation prompt')


# ---------------------------------------------------------------------------
# `ng site show`
# ---------------------------------------------------------------------------


async def _site_to_dict(site: Site) -> dict:
    sticky_groups, sticky_accounts, default_account, storage = (
        await asyncio.gather(
            resolve_group_names(site.group.sticky),
            resolve_slurm_account_labels(site.slurm.sticky),
            resolve_slurm_account_label(site.slurm.default_account),
            resolve_site_storage_settings(site.storage),
        )
    )
    return {
        'name': site.name,
        'fqdn': site.fqdn,
        'aliases': site.aliases,
        'sticky_groups': sticky_groups,
        'sticky_slurm_accounts': sticky_accounts,
        'default_slurm_account': default_account,
        'storage': storage,
        'created_at': site.created_at,
        'updated_at': site.updated_at,
    }


def _render_site_panel(data: dict) -> Panel:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()

    for key in ('name', 'fqdn', 'created_at', 'updated_at'):
        if data.get(key) is not None:
            table.add_row(key, str(data[key]))

    aliases = data.get('aliases')
    table.add_row('aliases', ', '.join(aliases) if aliases else '[dim](none)[/]')

    groups = data['sticky_groups']
    table.add_row(
        'sticky groups',
        '\n'.join(groups) if groups else '[dim](none)[/]',
    )
    accounts = data['sticky_slurm_accounts']
    table.add_row(
        'sticky slurm',
        '\n'.join(accounts) if accounts else '[dim](none)[/]',
    )
    default = data['default_slurm_account']
    table.add_row(
        'default slurm',
        default if default else '[dim](none)[/]',
    )

    storage = data['storage']
    table.add_row(
        'home volume',
        storage['default_home_volume'] or '[dim](none)[/]',
    )
    table.add_row(
        'home quota',
        storage['default_home_quota'] or '[dim](none)[/]',
    )
    if storage['home_automount_map']:
        mount = f'{storage["home_automount_map"]} [dim](automount)[/]'
    elif storage['home_static_mount']:
        mount = f'{storage["home_static_mount"]} [dim](static)[/]'
    else:
        mount = '[dim](none)[/]'
    table.add_row('home mount', mount)

    return Panel(
        table, title=f'[bold]Site:[/] [green]{data["name"]}[/]',
        border_style='green', expand=False,
    )


@site_args.apply(required=True)
@commands.register('ng', 'site', 'show',
                   help='Show site details: sticky groups + slurm accounts, '
                        'the default slurm account, and home-storage defaults')
async def site_show(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    data = await _site_to_dict(site)
    if args.yaml:
        print_yaml(data)
    else:
        console.print(_render_site_panel(data))


@site_show.args()
def _(parser: ArgParser):
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')


# ---------------------------------------------------------------------------
# `ng site {add,remove} sticky-group` — manage Site.group.sticky
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@group_args.apply(required=True, multiple=True)
@commands.register('ng', 'site', 'add', 'sticky-group',
                   help="Add Groups to site.group.sticky; every user at "
                        "the site is then implicitly a member")
async def site_add_sticky_group(args: Namespace):
    console = Console()

    async def add(group: str):
        await AddStickyGroup.run(
            args.db, args.author,
            sitename=args.site, groupname=group,
        )

    console.print(f'Adding to [bold]{args.site}[/].group.sticky:')
    return await run_per_target(console, args.group, add, ok='added')


@site_args.apply(required=True)
@group_args.apply(required=True, multiple=True)
@commands.register('ng', 'site', 'remove', 'sticky-group',
                   help='Remove Groups from site.group.sticky')
async def site_remove_sticky_group(args: Namespace):
    console = Console()

    async def remove(group: str):
        await RemoveStickyGroup.run(
            args.db, args.author,
            sitename=args.site, groupname=group,
        )

    console.print(f'Removing from [bold]{args.site}[/].group.sticky:')
    return await run_per_target(console, args.group, remove, ok='removed')


# ---------------------------------------------------------------------------
# `ng site {add,remove} sticky-slurm` — manage Site.slurm.sticky
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'site', 'add', 'sticky-slurm',
                   help="Add a group's SlurmAccount to site.slurm.sticky; "
                        "every user at the site is then implicitly a slurmer")
async def site_add_sticky_slurm(args: Namespace):
    console = Console()
    await AddStickySlurmAccount.run(
        args.db, args.author,
        sitename=args.site, groupname=args.group, default=args.default,
    )
    suffix = ' (and set as default_account)' if args.default else ''
    console.print(
        f'Added [green]{args.group}[/] to [bold]{args.site}[/].slurm.sticky{suffix}'
    )


@site_add_sticky_slurm.args()
def _(parser: ArgParser):
    parser.add_argument(
        '--default', action='store_true', default=False,
        help='Also set this account as site.slurm.default_account',
    )


@site_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'site', 'remove', 'sticky-slurm',
                   help="Remove a group's SlurmAccount from site.slurm.sticky")
async def site_remove_sticky_slurm(args: Namespace):
    console = Console()
    try:
        await RemoveStickySlurmAccount.run(
            args.db, args.author,
            sitename=args.site, groupname=args.group,
            clear_default=args.clear_default,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    suffix = ' (and cleared default_account)' if args.clear_default else ''
    console.print(
        f'Removed [yellow]{args.group}[/] from '
        f'[bold]{args.site}[/].slurm.sticky{suffix}'
    )


@site_remove_sticky_slurm.args()
def _(parser: ArgParser):
    parser.add_argument(
        '--clear-default', action='store_true', default=False,
        help='Clear site.slurm.default_account if it points at this account',
    )


# ---------------------------------------------------------------------------
# `ng site {add,remove} alias` — manage Site.aliases
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'site', 'add', 'alias',
                   help='Add aliases the site resolves by (name/fqdn/alias)')
async def site_add_alias(args: Namespace):
    console = Console()

    async def add(alias: str):
        await AddSiteAlias.run(
            args.db, args.author, sitename=args.site, alias=alias,
        )

    console.print(f'Adding alias(es) to [bold]{args.site}[/]:')
    return await run_per_target(console, args.alias, add, ok='added')


@site_add_alias.args()
def _(parser: ArgParser):
    parser.add_argument('alias', nargs='+', help='The alias(es) to add')


@site_args.apply(required=True)
@commands.register('ng', 'site', 'remove', 'alias',
                   help='Remove aliases from a site')
async def site_remove_alias(args: Namespace):
    console = Console()

    async def remove(alias: str):
        await RemoveSiteAlias.run(
            args.db, args.author, sitename=args.site, alias=alias,
        )

    console.print(f'Removing alias(es) from [bold]{args.site}[/]:')
    return await run_per_target(console, args.alias, remove, ok='removed')


@site_remove_alias.args()
def _(parser: ArgParser):
    parser.add_argument('alias', nargs='+', help='The alias(es) to remove')


# ---------------------------------------------------------------------------
# `ng site set slurm-default` — the site's default slurm account
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@group_args.apply()
@commands.register('ng', 'site', 'set', 'slurm-default',
                   help="Set a group's SlurmAccount as the site's default "
                        "account (also adds it to site.slurm.sticky if "
                        "needed), or --clear the default")
async def site_set_slurm_default(args: Namespace):
    console = Console()
    if bool(args.group) == bool(args.clear):
        console.print('[red]Pass exactly one of --group/-g or --clear[/]')
        return 1
    try:
        if args.clear:
            await ClearSiteDefaultSlurmAccount.run(
                args.db, args.author, sitename=args.site,
            )
        else:
            await SetSiteDefaultSlurmAccount.run(
                args.db, args.author,
                sitename=args.site, groupname=args.group,
            )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    if args.clear:
        console.print(
            f'Cleared the default slurm account for [bold]{args.site}[/]'
        )
    else:
        console.print(
            f'Set [green]{args.group}[/] as the default slurm account for '
            f'[bold]{args.site}[/]'
        )


@site_set_slurm_default.args()
def _(parser: ArgParser):
    parser.add_argument(
        '--clear', action='store_true', default=False,
        help="Clear the site's default slurm account "
             '(leaves it in site.slurm.sticky)',
    )


# ---------------------------------------------------------------------------
# `ng site set storage-defaults` — per-site storage defaults
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'site', 'set', 'storage-defaults',
                   help="Set the site's home-provisioning defaults "
                        "(parent volume, quota, mount mechanism)")
async def site_set_storage_defaults(args: Namespace):
    console = Console()
    if not any((args.home_volume, args.home_quota,
                args.home_automount_map, args.home_static_mount)):
        console.print('[red]Nothing to set; pass at least one option[/]')
        return 1
    if args.home_automount_map and args.home_static_mount:
        console.print(
            '[red]--home-automount-map and --home-static-mount are '
            'mutually exclusive[/]'
        )
        return 1
    try:
        await SetSiteStorageDefaults.run(
            args.db, args.author,
            sitename=args.site,
            home_volume=args.home_volume,
            home_quota=args.home_quota,
            home_automount_map=args.home_automount_map,
            home_static_mount=args.home_static_mount,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(f'Updated storage defaults for [bold]{args.site}[/]')


@site_set_storage_defaults.args()
def _(parser: ArgParser):
    parser.add_argument('--home-volume', default=None,
                        help='Parent volume new homes are provisioned under')
    parser.add_argument('--home-quota', default=None,
                        help='Default quota for new homes (e.g. 20G)')
    parser.add_argument('--home-automount-map', default=None,
                        help='Automount map used for new homes')
    parser.add_argument('--home-static-mount', default=None,
                        help='Static mount used for new homes')


# ---------------------------------------------------------------------------
# `ng site sync old-puppet` — sync site to the puppet.hpc YAML repo
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'site', 'sync', 'old-puppet',
                   help='Fully sync site from database to the puppet.hpc '
                        'YAML repo')
async def site_sync_old_puppet(args: Namespace):
    console = Console()
    try:
        result = await SyncOldPuppet.run(
            args.db, args.author,
            sitename=args.site,
            repo=args.repo,
            base_branch=args.base_branch,
            push=args.push_merge,
            write_keys=args.write_keys,
            delete_branch=args.delete_branch,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    except sh.ErrorReturnCode as e:
        console.print(
            f'[red]git failed:[/] {e.stderr.decode(errors="replace")}'
        )
        return 1

    if result['changed']:
        suffix = ' (pushed + merged)' if result['pushed'] else ' (no push)'
        console.print(
            f'Synced [bold]{args.site}[/] on branch '
            f'[green]{result["branch"]}[/]{suffix}'
        )
    else:
        console.print(f'[yellow]No changes for {args.site}[/]')


@site_sync_old_puppet.args()
def _(parser: ArgParser):
    parser.add_argument('repo', type=Path,
                        help='Path to the puppet.hpc repo clone')
    parser.add_argument('--base-branch', default='main')
    parser.add_argument('--push-merge', default=False, action='store_true',
                        help='Push the working branch, merge it into the '
                             'base branch, and push the merge')
    parser.add_argument('--write-keys', default=False, action='store_true',
                        help='Also write keys/<username>.pub files')
    parser.add_argument('--delete-branch', default=True,
                        action=BooleanOptionalAction,
                        help='Delete the working branch after a successful '
                             'push + merge')


# ---------------------------------------------------------------------------
# `ng site export` — read-only exports of site data
# ---------------------------------------------------------------------------


parent('ng', 'site', 'export', help='Export site data in various formats')


@site_args.apply(required=True)
@commands.register('ng', 'site', 'export', 'puppet-legacy',
                   help="Export site users/groups/storage as "
                        "v1-compatible puppet.hpc YAML")
async def site_export_puppet_legacy(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    puppet_map = await site_to_puppet_legacy(site)
    yaml_text = PuppetAccountMap.Schema().dumps(puppet_map)
    if args.output:
        Path(args.output).write_text(yaml_text)
        console.print(f'Wrote puppet YAML to [green]{args.output}[/]')
    else:
        console.print(highlight_yaml(yaml_text))


@site_export_puppet_legacy.args()
def _(parser: ArgParser):
    parser.add_argument('--output', '-o', default=None,
                        help='Write YAML to this path (default: stdout)')


@site_args.apply(required=True)
@commands.register('ng', 'site', 'export', 'root-keys',
                   help="Export root authorized_keys for admins with "
                        "root-ssh access at the site")
async def site_export_root_keys(args: Namespace):
    console = Console()
    try:
        blocks = await ExportRootSSHKeys.run(
            args.db, args.author, sitename=args.site,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    text = root_authorized_keys_text(blocks)
    if args.output:
        Path(args.output).write_text(text)
        console.print(f'Wrote root keys to [green]{args.output}[/]')
    else:
        # Raw to stdout so the key material is emitted verbatim (no Rich
        # markup reformatting).
        sys.stdout.write(text)


@site_export_root_keys.args()
def _(parser: ArgParser):
    parser.add_argument('--output', '-o', default=None,
                        help='Write authorized_keys to this path '
                             '(default: stdout)')


@site_args.apply(required=True)
@commands.register('ng', 'site', 'export', 'sympa',
                   help="Export site member emails (user/admin, not inactive) "
                        "in Sympa format, one per line")
async def site_export_sympa(args: Namespace):
    console = Console()
    try:
        text = await ExportSympaEmails.run(
            args.db, args.author, sitename=args.site, ignore=args.ignore,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    if args.output:
        Path(args.output).write_text(text)
        console.print(f'Wrote Sympa emails to [green]{args.output}[/]')
    else:
        sys.stdout.write(text)


@site_export_sympa.args()
def _(parser: ArgParser):
    parser.add_argument('--output', '-o', default=None,
                        help='Write emails to this path (default: stdout)')
    parser.add_argument('--ignore', nargs='+', default=['hpc-help@ucdavis.edu'],
                        help='Emails to exclude from the list')


@site_args.apply(required=True)
@commands.register('ng', 'site', 'export', 'storage',
                   help="Export site storage as legacy puppet zfs/nfs YAML "
                        "(v1 `db site to-puppet` storage format)")
async def site_export_storage(args: Namespace):
    console = Console()
    try:
        data = await ExportPuppetStorage.run(
            args.db, args.author, sitename=args.site,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    yaml_text = dumps_yaml(data)
    if args.output:
        Path(args.output).write_text(yaml_text)
        console.print(f'Wrote puppet storage YAML to [green]{args.output}[/]')
    else:
        sys.stdout.write(yaml_text)


@site_export_storage.args()
def _(parser: ArgParser):
    parser.add_argument('--output', '-o', default=None,
                        help='Write YAML to this path (default: stdout)')
