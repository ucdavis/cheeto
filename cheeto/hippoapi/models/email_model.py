from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="EmailModel")


@_attrs_define
class EmailModel:
    """
    Attributes:
        emails (list[str]):
        cc_emails (list[str]):
        subject (str):
        text_body (str):
        html_body (None | str | Unset):
    """

    emails: list[str]
    cc_emails: list[str]
    subject: str
    text_body: str
    html_body: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        emails = self.emails

        cc_emails = self.cc_emails

        subject = self.subject

        text_body = self.text_body

        html_body: None | str | Unset
        if isinstance(self.html_body, Unset):
            html_body = UNSET
        else:
            html_body = self.html_body

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "emails": emails,
                "ccEmails": cc_emails,
                "subject": subject,
                "textBody": text_body,
            }
        )
        if html_body is not UNSET:
            field_dict["htmlBody"] = html_body

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        emails = cast(list[str], d.pop("emails"))

        cc_emails = cast(list[str], d.pop("ccEmails"))

        subject = d.pop("subject")

        text_body = d.pop("textBody")

        def _parse_html_body(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        html_body = _parse_html_body(d.pop("htmlBody", UNSET))

        email_model = cls(
            emails=emails,
            cc_emails=cc_emails,
            subject=subject,
            text_body=text_body,
            html_body=html_body,
        )

        return email_model
