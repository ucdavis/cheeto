"""REST API key lookups."""

from ..models.apikey import ApiKey


async def find_api_key(key_id: str) -> ApiKey | None:
    """One key by its public id (unique index); revoked keys included."""
    return await ApiKey.find_one(ApiKey.key_id == key_id)


async def list_api_keys(*, include_revoked: bool = False) -> list[ApiKey]:
    """Keys sorted by name, then by creation time. Expired keys are
    included: an expired key is still a record somebody must revoke or
    replace."""
    filters = [] if include_revoked else [ApiKey.revoked_at == None]  # noqa: E711
    return await ApiKey.find(*filters).sort('+name', '+created_at').to_list()
