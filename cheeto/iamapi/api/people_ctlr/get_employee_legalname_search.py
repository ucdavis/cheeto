from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    l_first_name: Union[Unset, str] = UNSET,
    l_middle_name: Union[Unset, str] = UNSET,
    l_last_name: Union[Unset, str] = UNSET,
    employee_id: Union[Unset, str] = UNSET,
    mothra_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    modify_date_after: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["lFirstName"] = l_first_name

    params["lMiddleName"] = l_middle_name

    params["lLastName"] = l_last_name

    params["employeeId"] = employee_id

    params["mothraId"] = mothra_id

    params["studentId"] = student_id

    params["externalId"] = external_id

    params["iamId"] = iam_id

    params["bannerPIdM"] = banner_p_id_m

    params["modifyDateAfter"] = modify_date_after

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/iam/people/legalname/employees/search",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Any]:
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Any]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    l_first_name: Union[Unset, str] = UNSET,
    l_middle_name: Union[Unset, str] = UNSET,
    l_last_name: Union[Unset, str] = UNSET,
    employee_id: Union[Unset, str] = UNSET,
    mothra_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    modify_date_after: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM People legalname

     This API will need a special key that will be granted after an approval before a user can call it
     Provides basic person information about the requested person/people

    Args:
        l_first_name (Union[Unset, str]):
        l_middle_name (Union[Unset, str]):
        l_last_name (Union[Unset, str]):
        employee_id (Union[Unset, str]):
        mothra_id (Union[Unset, str]):
        student_id (Union[Unset, str]):
        external_id (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        banner_p_id_m (Union[Unset, str]):
        modify_date_after (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        l_first_name=l_first_name,
        l_middle_name=l_middle_name,
        l_last_name=l_last_name,
        employee_id=employee_id,
        mothra_id=mothra_id,
        student_id=student_id,
        external_id=external_id,
        iam_id=iam_id,
        banner_p_id_m=banner_p_id_m,
        modify_date_after=modify_date_after,
        key=key,
        v=v,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    l_first_name: Union[Unset, str] = UNSET,
    l_middle_name: Union[Unset, str] = UNSET,
    l_last_name: Union[Unset, str] = UNSET,
    employee_id: Union[Unset, str] = UNSET,
    mothra_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    modify_date_after: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM People legalname

     This API will need a special key that will be granted after an approval before a user can call it
     Provides basic person information about the requested person/people

    Args:
        l_first_name (Union[Unset, str]):
        l_middle_name (Union[Unset, str]):
        l_last_name (Union[Unset, str]):
        employee_id (Union[Unset, str]):
        mothra_id (Union[Unset, str]):
        student_id (Union[Unset, str]):
        external_id (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        banner_p_id_m (Union[Unset, str]):
        modify_date_after (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        l_first_name=l_first_name,
        l_middle_name=l_middle_name,
        l_last_name=l_last_name,
        employee_id=employee_id,
        mothra_id=mothra_id,
        student_id=student_id,
        external_id=external_id,
        iam_id=iam_id,
        banner_p_id_m=banner_p_id_m,
        modify_date_after=modify_date_after,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
