"""SendUserEmail — the audit-logged choke point for every user-facing email.

All user emails (IAM lifecycle notifications and the HiPPO event-handler
emails) go through this operation so the fully-rendered message — subject,
header, recipients, and body — is recorded in the History collection. Sending
is best-effort: a non-200 or transport failure is captured as ``sent=False``
and still logged, so a failed notification never aborts the sync or handler
that triggered it.
"""

from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..hippo import send_email
from ..hippoapi.client import AuthenticatedClient
from ..mail import Email
from ..models.user import User
from .base import Operation


class SendUserEmail(Operation):
    """Send a rendered `Email` via the HiPPO notification transport and record
    the full rendered text in History."""

    op_name = 'send_user_email'

    # External, non-rollbackable API I/O; also invoked from inside other,
    # already-committed flows (IAM sync driver, HiPPO handlers), so it must not
    # hold a transaction of its own.
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        mail: Email,
        hippo_client: AuthenticatedClient,
    ) -> None:
        super().__init__(client, author)
        self.mail = mail
        self.hippo_client = hippo_client
        self._sent = False

    async def execute(self, session: AsyncClientSession) -> bool:
        self._sent = await send_email(self.mail, self.hippo_client)
        return self._sent

    def describe(self) -> dict[str, Any]:
        return {
            'email_type': type(self.mail).__name__,
            'subject': self.mail.subject,
            'header': self.mail.header,
            'to': list(self.mail.emails),
            'cc': list(self.mail.ccEmails),
            'body': '\n\n'.join(self.mail.paragraphs()),
            'sent': self._sent,
        }
