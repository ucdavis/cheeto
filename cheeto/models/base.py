from datetime import datetime, timezone
from typing import Annotated

from beanie import Document, Insert, PydanticObjectId, Replace, Save, SaveChanges, Update, before_event
from bson import DBRef, ObjectId
from pydantic import BaseModel, BeforeValidator, Field


def link_target_id(link) -> PydanticObjectId | None:
    """Target id of a `Link[X]` (fetched or not), a raw ObjectId/DocRef, or
    None. Use anywhere a document reference needs to be compared by identity
    without a round-trip to materialize it."""
    if link is None:
        return None
    if isinstance(link, ObjectId):  # PydanticObjectId subclasses ObjectId
        return link
    ref = getattr(link, 'ref', None)
    if ref is not None:
        return ref.id
    return link.id


def coerce_to_object_id(v) -> PydanticObjectId | None:
    """Coerce any document-reference shape to a bare PydanticObjectId.

    Accepts: ObjectId/PydanticObjectId, beanie Link, bson DBRef, a fetched
    Document (anything with an ObjectId `.id`), a legacy inline-snapshot
    dict (`{'_id': ...}` — see `DocRef` for why these exist), or a str.
    """
    if v is None:
        return None
    if isinstance(v, ObjectId):
        return PydanticObjectId(v)
    ref = getattr(v, 'ref', None)
    if isinstance(ref, DBRef):  # beanie Link proxy
        return PydanticObjectId(ref.id)
    if isinstance(v, DBRef):
        return PydanticObjectId(v.id)
    doc_id = getattr(v, 'id', None)
    if isinstance(doc_id, ObjectId):  # fetched Document
        return PydanticObjectId(doc_id)
    if isinstance(v, dict) and '_id' in v:  # legacy inline snapshot
        return PydanticObjectId(v['_id'])
    if isinstance(v, str):
        return PydanticObjectId(v)
    raise ValueError(f'cannot coerce {v!r} to an ObjectId document reference')


# A document reference stored as a bare ObjectId, for use in EMBEDDED models.
#
# RULE: `Link[...]` (and `BackLink`) may only be declared on Document
# classes. Beanie discovers link fields by walking each Document's own
# top-level model_fields at init time — a Link nested inside an embedded
# BaseModel is invisible to it, so beanie silently stores the linked
# document as an INLINE SNAPSHOT and rehydrates it as a full document (no
# `.ref`), producing stale copies and `.ref` AttributeErrors. Embedded
# models must reference documents with `DocRef` instead; the validator
# coerces Links/Documents/DBRefs (and legacy snapshot dicts, self-healing
# previously-damaged rows on read) to the bare id. Resolve with
# `Model.get(...)` / `In(Model.id, ...)`; compare with `link_target_id`.
# A tripwire test in test_beanie.py enforces the rule.
DocRef = Annotated[PydanticObjectId, BeforeValidator(coerce_to_object_id)]


class BaseDocument(Document):
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @before_event(Replace, Save, SaveChanges, Update)
    def update_timestamp(self):
        self.updated_at = datetime.now(timezone.utc)

    class Settings:
        use_state_management = True
        is_root = False


class Expirable(BaseModel):
    """Mixin providing optional expires_at and provisioned_at timestamps."""

    expires_at: datetime | None = None
    provisioned_at: datetime | None = None
