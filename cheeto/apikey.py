"""REST API keys: format, generation and hashing. No database code here;
the document is `cheeto/models/apikey.py`.

A key is `cheeto_<key id>_<secret>`:

- the prefix makes a leaked key easy to recognize and to search for;
- the key id (12 hex characters) finds the record, and is safe to show,
  list and log;
- the secret is 256 random bits. Only its SHA-256 is stored. A fast hash
  is correct for a secret of this size: a slow password hash would cost
  time on every request and protect nothing, because nobody can guess the
  input.

The full key is shown one time, when it is issued.
"""

import hashlib
import re
import secrets

from .constants import API_KEY_PREFIX, API_SCOPE_ALL, API_SCOPES

_KEY_ID_RE = re.compile(r'^[0-9a-f]{12}$')


def hash_api_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def generate_api_key() -> tuple[str, str, str]:
    """A new key as `(token, key_id, secret_hash)`. `token` is what the
    client sends; store only the other two."""
    key_id = secrets.token_hex(6)
    secret = secrets.token_urlsafe(32)
    return (f'{API_KEY_PREFIX}_{key_id}_{secret}', key_id,
            hash_api_secret(secret))


def parse_api_key(token: str) -> tuple[str, str] | None:
    """Split a token into `(key_id, secret)`; None when it does not have
    the key format."""
    # maxsplit: the secret is urlsafe base64 and can contain '_'.
    parts = token.split('_', 2)
    if len(parts) != 3:
        return None
    prefix, key_id, secret = parts
    if prefix != API_KEY_PREFIX or not secret:
        return None
    if _KEY_ID_RE.match(key_id) is None:
        return None
    return key_id, secret


def normalize_scopes(scopes) -> list[str]:
    """Validate a scope list and return it sorted without duplicates.
    `ALL` includes every scope, so it replaces the rest. Raises ValueError
    for an empty list or an unknown scope."""
    scopes = set(scopes)
    if not scopes:
        raise ValueError('an API key needs at least one scope')
    unknown = scopes - set(API_SCOPES) - {API_SCOPE_ALL}
    if unknown:
        raise ValueError(
            f'unknown API scope(s): {", ".join(sorted(unknown))}; '
            f'valid: {", ".join(API_SCOPES)}, or {API_SCOPE_ALL} for '
            f'every scope'
        )
    if API_SCOPE_ALL in scopes:
        return [API_SCOPE_ALL]
    return sorted(scopes)
