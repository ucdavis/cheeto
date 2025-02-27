from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define

from ..models.financial_chart_string_type import FinancialChartStringType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.gl_segments import GlSegments
    from ..models.key_value_pair_of_string_and_string import (
        KeyValuePairOfStringAndString,
    )
    from ..models.ppm_segments import PpmSegments


T = TypeVar("T", bound="ChartStringValidationModel")


@_attrs_define
class ChartStringValidationModel:
    """
    Attributes:
        is_valid (Union[Unset, bool]):
        chart_string (Union[None, Unset, str]):
        chart_type (Union[Unset, FinancialChartStringType]):
        gl_segments (Union['GlSegments', None, Unset]):
        ppm_segments (Union['PpmSegments', None, Unset]):
        account_manager (Union[None, Unset, str]):
        account_manager_email (Union[None, Unset, str]):
        description (Union[None, Unset, str]):
        details (Union[None, Unset, list['KeyValuePairOfStringAndString']]):
        warnings (Union[None, Unset, list['KeyValuePairOfStringAndString']]):
        message (Union[None, Unset, str]):
        messages (Union[None, Unset, list[str]]):
        warning (Union[None, Unset, str]):
    """

    is_valid: Union[Unset, bool] = UNSET
    chart_string: Union[None, Unset, str] = UNSET
    chart_type: Union[Unset, FinancialChartStringType] = UNSET
    gl_segments: Union["GlSegments", None, Unset] = UNSET
    ppm_segments: Union["PpmSegments", None, Unset] = UNSET
    account_manager: Union[None, Unset, str] = UNSET
    account_manager_email: Union[None, Unset, str] = UNSET
    description: Union[None, Unset, str] = UNSET
    details: Union[None, Unset, list["KeyValuePairOfStringAndString"]] = UNSET
    warnings: Union[None, Unset, list["KeyValuePairOfStringAndString"]] = UNSET
    message: Union[None, Unset, str] = UNSET
    messages: Union[None, Unset, list[str]] = UNSET
    warning: Union[None, Unset, str] = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.gl_segments import GlSegments
        from ..models.ppm_segments import PpmSegments

        is_valid = self.is_valid

        chart_string: Union[None, Unset, str]
        if isinstance(self.chart_string, Unset):
            chart_string = UNSET
        else:
            chart_string = self.chart_string

        chart_type: Union[Unset, int] = UNSET
        if not isinstance(self.chart_type, Unset):
            chart_type = self.chart_type.value

        gl_segments: Union[None, Unset, dict[str, Any]]
        if isinstance(self.gl_segments, Unset):
            gl_segments = UNSET
        elif isinstance(self.gl_segments, GlSegments):
            gl_segments = self.gl_segments.to_dict()
        else:
            gl_segments = self.gl_segments

        ppm_segments: Union[None, Unset, dict[str, Any]]
        if isinstance(self.ppm_segments, Unset):
            ppm_segments = UNSET
        elif isinstance(self.ppm_segments, PpmSegments):
            ppm_segments = self.ppm_segments.to_dict()
        else:
            ppm_segments = self.ppm_segments

        account_manager: Union[None, Unset, str]
        if isinstance(self.account_manager, Unset):
            account_manager = UNSET
        else:
            account_manager = self.account_manager

        account_manager_email: Union[None, Unset, str]
        if isinstance(self.account_manager_email, Unset):
            account_manager_email = UNSET
        else:
            account_manager_email = self.account_manager_email

        description: Union[None, Unset, str]
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        details: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.details, Unset):
            details = UNSET
        elif isinstance(self.details, list):
            details = []
            for details_type_0_item_data in self.details:
                details_type_0_item = details_type_0_item_data.to_dict()
                details.append(details_type_0_item)

        else:
            details = self.details

        warnings: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.warnings, Unset):
            warnings = UNSET
        elif isinstance(self.warnings, list):
            warnings = []
            for warnings_type_0_item_data in self.warnings:
                warnings_type_0_item = warnings_type_0_item_data.to_dict()
                warnings.append(warnings_type_0_item)

        else:
            warnings = self.warnings

        message: Union[None, Unset, str]
        if isinstance(self.message, Unset):
            message = UNSET
        else:
            message = self.message

        messages: Union[None, Unset, list[str]]
        if isinstance(self.messages, Unset):
            messages = UNSET
        elif isinstance(self.messages, list):
            messages = self.messages

        else:
            messages = self.messages

        warning: Union[None, Unset, str]
        if isinstance(self.warning, Unset):
            warning = UNSET
        else:
            warning = self.warning

        field_dict: dict[str, Any] = {}
        field_dict.update({})
        if is_valid is not UNSET:
            field_dict["isValid"] = is_valid
        if chart_string is not UNSET:
            field_dict["chartString"] = chart_string
        if chart_type is not UNSET:
            field_dict["chartType"] = chart_type
        if gl_segments is not UNSET:
            field_dict["glSegments"] = gl_segments
        if ppm_segments is not UNSET:
            field_dict["ppmSegments"] = ppm_segments
        if account_manager is not UNSET:
            field_dict["accountManager"] = account_manager
        if account_manager_email is not UNSET:
            field_dict["accountManagerEmail"] = account_manager_email
        if description is not UNSET:
            field_dict["description"] = description
        if details is not UNSET:
            field_dict["details"] = details
        if warnings is not UNSET:
            field_dict["warnings"] = warnings
        if message is not UNSET:
            field_dict["message"] = message
        if messages is not UNSET:
            field_dict["messages"] = messages
        if warning is not UNSET:
            field_dict["warning"] = warning

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        from ..models.gl_segments import GlSegments
        from ..models.key_value_pair_of_string_and_string import (
            KeyValuePairOfStringAndString,
        )
        from ..models.ppm_segments import PpmSegments

        d = src_dict.copy()
        is_valid = d.pop("isValid", UNSET)

        def _parse_chart_string(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        chart_string = _parse_chart_string(d.pop("chartString", UNSET))

        _chart_type = d.pop("chartType", UNSET)
        chart_type: Union[Unset, FinancialChartStringType]
        if isinstance(_chart_type, Unset):
            chart_type = UNSET
        else:
            chart_type = FinancialChartStringType(_chart_type)

        def _parse_gl_segments(data: object) -> Union["GlSegments", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                gl_segments_type_0 = GlSegments.from_dict(data)

                return gl_segments_type_0
            except:  # noqa: E722
                pass
            return cast(Union["GlSegments", None, Unset], data)

        gl_segments = _parse_gl_segments(d.pop("glSegments", UNSET))

        def _parse_ppm_segments(data: object) -> Union["PpmSegments", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                ppm_segments_type_0 = PpmSegments.from_dict(data)

                return ppm_segments_type_0
            except:  # noqa: E722
                pass
            return cast(Union["PpmSegments", None, Unset], data)

        ppm_segments = _parse_ppm_segments(d.pop("ppmSegments", UNSET))

        def _parse_account_manager(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        account_manager = _parse_account_manager(d.pop("accountManager", UNSET))

        def _parse_account_manager_email(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        account_manager_email = _parse_account_manager_email(
            d.pop("accountManagerEmail", UNSET)
        )

        def _parse_description(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        description = _parse_description(d.pop("description", UNSET))

        def _parse_details(
            data: object,
        ) -> Union[None, Unset, list["KeyValuePairOfStringAndString"]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                details_type_0 = []
                _details_type_0 = data
                for details_type_0_item_data in _details_type_0:
                    details_type_0_item = KeyValuePairOfStringAndString.from_dict(
                        details_type_0_item_data
                    )

                    details_type_0.append(details_type_0_item)

                return details_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list["KeyValuePairOfStringAndString"]], data)

        details = _parse_details(d.pop("details", UNSET))

        def _parse_warnings(
            data: object,
        ) -> Union[None, Unset, list["KeyValuePairOfStringAndString"]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                warnings_type_0 = []
                _warnings_type_0 = data
                for warnings_type_0_item_data in _warnings_type_0:
                    warnings_type_0_item = KeyValuePairOfStringAndString.from_dict(
                        warnings_type_0_item_data
                    )

                    warnings_type_0.append(warnings_type_0_item)

                return warnings_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list["KeyValuePairOfStringAndString"]], data)

        warnings = _parse_warnings(d.pop("warnings", UNSET))

        def _parse_message(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        message = _parse_message(d.pop("message", UNSET))

        def _parse_messages(data: object) -> Union[None, Unset, list[str]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                messages_type_0 = cast(list[str], data)

                return messages_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[str]], data)

        messages = _parse_messages(d.pop("messages", UNSET))

        def _parse_warning(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        warning = _parse_warning(d.pop("warning", UNSET))

        chart_string_validation_model = cls(
            is_valid=is_valid,
            chart_string=chart_string,
            chart_type=chart_type,
            gl_segments=gl_segments,
            ppm_segments=ppm_segments,
            account_manager=account_manager,
            account_manager_email=account_manager_email,
            description=description,
            details=details,
            warnings=warnings,
            message=message,
            messages=messages,
            warning=warning,
        )

        return chart_string_validation_model
