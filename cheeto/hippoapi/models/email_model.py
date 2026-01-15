from typing import Any, TypeVar, Union, cast

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
        html_body (Union[None, Unset, str]):
    """

    emails: list[str]
    cc_emails: list[str]
    subject: str
    text_body: str
    html_body: Union[None, Unset, str] = UNSET

    def to_dict(self) -> dict[str, Any]:
        emails = self.emails

        cc_emails = self.cc_emails

        subject = self.subject

        text_body = self.text_body

        html_body: Union[None, Unset, str]
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
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        d = src_dict.copy()
        emails = cast(list[str], d.pop("emails"))

        cc_emails = cast(list[str], d.pop("ccEmails"))

        subject = d.pop("subject")

        text_body = d.pop("textBody")

        def _parse_html_body(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        html_body = _parse_html_body(d.pop("htmlBody", UNSET))

        email_model = cls(
            emails=emails,
            cc_emails=cc_emails,
            subject=subject,
            text_body=text_body,
            html_body=html_body,
        )

        return email_model
