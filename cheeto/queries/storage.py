"""Storage-related read-only query helpers.

Pure read functions used by the LDAP sync operations to find Storage rows
that need to be projected as automount entries.
"""

from __future__ import annotations

from ..models.site import Site
from ..models.storage import Storage


async def list_automap_storages(site: Site, category: str) -> list[Storage]:
    """List Storage rows at `site` whose category matches and that have an
    automount_map (i.e. are intended to be projected into LDAP autofs).

    `category` is one of 'home' / 'group' (matching v1's
    `query_automap_storages` semantics)."""
    return await Storage.find(
        Storage.site.id == site.id,
        Storage.category == category,
        Storage.automount_map != None,  # noqa: E711 — beanie operator quirk
        fetch_links=True,
        nesting_depth=1,
    ).to_list()


async def list_admin_root_ssh_keys(site: Site) -> list[str]:
    """Collect SSH keys from active admin users on `site` that have
    'root-ssh' access. Used by SyncUserToLDAP for `type='system'` users
    so admins can ssh-as-root via the system account."""
    from ..models.group import AccessGroup
    from ..models.user import SshKey, User
    from ..models.user_site_info import UserSiteInfo

    root_ssh = await AccessGroup.find_one(
        AccessGroup.access_name == 'root-ssh',
    )
    if root_ssh is None:
        return []

    # Admin users on this site with root-ssh access (global or per-site).
    admins = await User.find(
        User.type == 'admin',
        User.access.id == root_ssh.id,
    ).to_list()
    if not admins:
        return []

    on_site_ids: set = set()
    for admin in admins:
        usi = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == admin.id,
            UserSiteInfo.site.id == site.id,
        )
        if usi is not None:
            on_site_ids.add(admin.id)

    if not on_site_ids:
        return []

    keys: list[str] = []
    for admin in admins:
        if admin.id not in on_site_ids:
            continue
        admin_keys = await SshKey.find(SshKey.user.id == admin.id).to_list()
        keys.extend(k.key for k in admin_keys)
    return keys
