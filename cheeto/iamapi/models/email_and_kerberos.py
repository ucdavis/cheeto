from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EmailAndKerberos")


@_attrs_define
class EmailAndKerberos:
    """
    Attributes:
        response_details (Union[Unset, str]):
        response_status (Union[Unset, int]):
        email (Union[Unset, str]):
        kerberos_id (Union[Unset, str]):
        eid (Union[Unset, str]):
        api_deprecated_error (Union[Unset, str]):
    """

    response_details: Union[Unset, str] = UNSET
    response_status: Union[Unset, int] = UNSET
    email: Union[Unset, str] = UNSET
    kerberos_id: Union[Unset, str] = UNSET
    eid: Union[Unset, str] = UNSET
    api_deprecated_error: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        response_details = self.response_details

        response_status = self.response_status

        email = self.email

        kerberos_id = self.kerberos_id

        eid = self.eid

        api_deprecated_error = self.api_deprecated_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if response_details is not UNSET:
            field_dict["responseDetails"] = response_details
        if response_status is not UNSET:
            field_dict["responseStatus"] = response_status
        if email is not UNSET:
            field_dict["email"] = email
        if kerberos_id is not UNSET:
            field_dict["kerberosID"] = kerberos_id
        if eid is not UNSET:
            field_dict["eid"] = eid
        if api_deprecated_error is not UNSET:
            field_dict["apiDeprecatedError"] = api_deprecated_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        d = src_dict.copy()
        response_details = d.pop("responseDetails", UNSET)

        response_status = d.pop("responseStatus", UNSET)

        email = d.pop("email", UNSET)

        kerberos_id = d.pop("kerberosID", UNSET)

        eid = d.pop("eid", UNSET)

        api_deprecated_error = d.pop("apiDeprecatedError", UNSET)

        email_and_kerberos = cls(
            response_details=response_details,
            response_status=response_status,
            email=email,
            kerberos_id=kerberos_id,
            eid=eid,
            api_deprecated_error=api_deprecated_error,
        )

        email_and_kerberos.additional_properties = d
        return email_and_kerberos

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
