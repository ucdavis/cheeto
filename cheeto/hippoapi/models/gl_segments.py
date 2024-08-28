from typing import Any, Dict, Type, TypeVar, Union

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GlSegments")


@_attrs_define
class GlSegments:
    """
    Attributes:
        account (Union[Unset, str]):
        activity (Union[Unset, str]):
        department (Union[Unset, str]):
        entity (Union[Unset, str]):
        fund (Union[Unset, str]):
        program (Union[Unset, str]):
        project (Union[Unset, str]):
        purpose (Union[Unset, str]):
        inter_entity (Union[Unset, str]):
        flex1 (Union[Unset, str]):
        flex2 (Union[Unset, str]):
    """

    account: Union[Unset, str] = UNSET
    activity: Union[Unset, str] = UNSET
    department: Union[Unset, str] = UNSET
    entity: Union[Unset, str] = UNSET
    fund: Union[Unset, str] = UNSET
    program: Union[Unset, str] = UNSET
    project: Union[Unset, str] = UNSET
    purpose: Union[Unset, str] = UNSET
    inter_entity: Union[Unset, str] = UNSET
    flex1: Union[Unset, str] = UNSET
    flex2: Union[Unset, str] = UNSET

    def to_dict(self) -> Dict[str, Any]:
        account = self.account

        activity = self.activity

        department = self.department

        entity = self.entity

        fund = self.fund

        program = self.program

        project = self.project

        purpose = self.purpose

        inter_entity = self.inter_entity

        flex1 = self.flex1

        flex2 = self.flex2

        field_dict: Dict[str, Any] = {}
        field_dict.update({})
        if account is not UNSET:
            field_dict["account"] = account
        if activity is not UNSET:
            field_dict["activity"] = activity
        if department is not UNSET:
            field_dict["department"] = department
        if entity is not UNSET:
            field_dict["entity"] = entity
        if fund is not UNSET:
            field_dict["fund"] = fund
        if program is not UNSET:
            field_dict["program"] = program
        if project is not UNSET:
            field_dict["project"] = project
        if purpose is not UNSET:
            field_dict["purpose"] = purpose
        if inter_entity is not UNSET:
            field_dict["interEntity"] = inter_entity
        if flex1 is not UNSET:
            field_dict["flex1"] = flex1
        if flex2 is not UNSET:
            field_dict["flex2"] = flex2

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        account = d.pop("account", UNSET)

        activity = d.pop("activity", UNSET)

        department = d.pop("department", UNSET)

        entity = d.pop("entity", UNSET)

        fund = d.pop("fund", UNSET)

        program = d.pop("program", UNSET)

        project = d.pop("project", UNSET)

        purpose = d.pop("purpose", UNSET)

        inter_entity = d.pop("interEntity", UNSET)

        flex1 = d.pop("flex1", UNSET)

        flex2 = d.pop("flex2", UNSET)

        gl_segments = cls(
            account=account,
            activity=activity,
            department=department,
            entity=entity,
            fund=fund,
            program=program,
            project=project,
            purpose=purpose,
            inter_entity=inter_entity,
            flex1=flex1,
            flex2=flex2,
        )

        return gl_segments
