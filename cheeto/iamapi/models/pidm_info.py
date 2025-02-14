from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="PIDMInfo")


@_attrs_define
class PIDMInfo:
    """
    Attributes:
        response_details (Union[Unset, str]):
        response_status (Union[Unset, int]):
        loginid (Union[Unset, str]):
        pidm (Union[Unset, str]):
        api_deprecated_error (Union[Unset, str]):
    """

    response_details: Union[Unset, str] = UNSET
    response_status: Union[Unset, int] = UNSET
    loginid: Union[Unset, str] = UNSET
    pidm: Union[Unset, str] = UNSET
    api_deprecated_error: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        response_details = self.response_details

        response_status = self.response_status

        loginid = self.loginid

        pidm = self.pidm

        api_deprecated_error = self.api_deprecated_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if response_details is not UNSET:
            field_dict["responseDetails"] = response_details
        if response_status is not UNSET:
            field_dict["responseStatus"] = response_status
        if loginid is not UNSET:
            field_dict["loginid"] = loginid
        if pidm is not UNSET:
            field_dict["pidm"] = pidm
        if api_deprecated_error is not UNSET:
            field_dict["apiDeprecatedError"] = api_deprecated_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        d = src_dict.copy()
        response_details = d.pop("responseDetails", UNSET)

        response_status = d.pop("responseStatus", UNSET)

        loginid = d.pop("loginid", UNSET)

        pidm = d.pop("pidm", UNSET)

        api_deprecated_error = d.pop("apiDeprecatedError", UNSET)

        pidm_info = cls(
            response_details=response_details,
            response_status=response_status,
            loginid=loginid,
            pidm=pidm,
            api_deprecated_error=api_deprecated_error,
        )

        pidm_info.additional_properties = d
        return pidm_info

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
