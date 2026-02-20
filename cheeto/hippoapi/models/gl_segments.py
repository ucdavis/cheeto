from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GlSegments")


@_attrs_define
class GlSegments:
    """
    Attributes:
        account (str | Unset):
        activity (str | Unset):
        department (str | Unset):
        entity (str | Unset):
        fund (str | Unset):
        program (str | Unset):
        project (str | Unset):
        purpose (str | Unset):
        inter_entity (str | Unset):
        flex1 (str | Unset):
        flex2 (str | Unset):
    """

    account: str | Unset = UNSET
    activity: str | Unset = UNSET
    department: str | Unset = UNSET
    entity: str | Unset = UNSET
    fund: str | Unset = UNSET
    program: str | Unset = UNSET
    project: str | Unset = UNSET
    purpose: str | Unset = UNSET
    inter_entity: str | Unset = UNSET
    flex1: str | Unset = UNSET
    flex2: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
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

        field_dict: dict[str, Any] = {}

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
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
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
