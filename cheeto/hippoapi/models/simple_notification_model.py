from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SimpleNotificationModel")


@_attrs_define
class SimpleNotificationModel:
    """
    Attributes:
        emails (list[str]):
        cc_emails (list[str]):
        subject (str):
        paragraphs (list[str]):
        header (Union[None, Unset, str]):
    """

    emails: list[str]
    cc_emails: list[str]
    subject: str
    paragraphs: list[str]
    header: Union[None, Unset, str] = UNSET

    def to_dict(self) -> dict[str, Any]:
        emails = self.emails

        cc_emails = self.cc_emails

        subject = self.subject

        paragraphs = self.paragraphs

        header: Union[None, Unset, str]
        if isinstance(self.header, Unset):
            header = UNSET
        else:
            header = self.header

        field_dict: dict[str, Any] = {}
        field_dict.update(
            {
                "emails": emails,
                "ccEmails": cc_emails,
                "subject": subject,
                "paragraphs": paragraphs,
            }
        )
        if header is not UNSET:
            field_dict["header"] = header

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        d = src_dict.copy()
        emails = cast(list[str], d.pop("emails"))

        cc_emails = cast(list[str], d.pop("ccEmails"))

        subject = d.pop("subject")

        paragraphs = cast(list[str], d.pop("paragraphs"))

        def _parse_header(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        header = _parse_header(d.pop("header", UNSET))

        simple_notification_model = cls(
            emails=emails,
            cc_emails=cc_emails,
            subject=subject,
            paragraphs=paragraphs,
            header=header,
        )

        return simple_notification_model
