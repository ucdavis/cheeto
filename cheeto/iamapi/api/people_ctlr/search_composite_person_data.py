from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    o_first_name: Union[Unset, str] = UNSET,
    o_middle_name: Union[Unset, str] = UNSET,
    o_last_name: Union[Unset, str] = UNSET,
    d_first_name: Union[Unset, str] = UNSET,
    d_middle_name: Union[Unset, str] = UNSET,
    employee_id: Union[Unset, str] = UNSET,
    d_last_name: Union[Unset, str] = UNSET,
    mothra_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    pps_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    dept_code: Union[Unset, str] = UNSET,
    title_code: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}

    params["oFirstName"] = o_first_name

    params["oMiddleName"] = o_middle_name

    params["oLastName"] = o_last_name

    params["dFirstName"] = d_first_name

    params["dMiddleName"] = d_middle_name

    params["employeeId"] = employee_id

    params["dLastName"] = d_last_name

    params["mothraId"] = mothra_id

    params["studentId"] = student_id

    params["externalId"] = external_id

    params["iamId"] = iam_id

    params["ppsId"] = pps_id

    params["bannerPIdM"] = banner_p_id_m

    params["userId"] = user_id

    params["email"] = email

    params["deptCode"] = dept_code

    params["titleCode"] = title_code

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: Dict[str, Any] = {
        "method": "get",
        "url": "/iam/people/composite/search",
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
    o_first_name: Union[Unset, str] = UNSET,
    o_middle_name: Union[Unset, str] = UNSET,
    o_last_name: Union[Unset, str] = UNSET,
    d_first_name: Union[Unset, str] = UNSET,
    d_middle_name: Union[Unset, str] = UNSET,
    employee_id: Union[Unset, str] = UNSET,
    d_last_name: Union[Unset, str] = UNSET,
    mothra_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    pps_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    dept_code: Union[Unset, str] = UNSET,
    title_code: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM People Composite

     Kitchen Sink Query, gets almost all the information available for all people requested. Limited
    acces since this query is resource intensive.

    Args:
        o_first_name (Union[Unset, str]):
        o_middle_name (Union[Unset, str]):
        o_last_name (Union[Unset, str]):
        d_first_name (Union[Unset, str]):
        d_middle_name (Union[Unset, str]):
        employee_id (Union[Unset, str]):
        d_last_name (Union[Unset, str]):
        mothra_id (Union[Unset, str]):
        student_id (Union[Unset, str]):
        external_id (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        pps_id (Union[Unset, str]):
        banner_p_id_m (Union[Unset, str]):
        user_id (Union[Unset, str]):
        email (Union[Unset, str]):
        dept_code (Union[Unset, str]):
        title_code (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        o_first_name=o_first_name,
        o_middle_name=o_middle_name,
        o_last_name=o_last_name,
        d_first_name=d_first_name,
        d_middle_name=d_middle_name,
        employee_id=employee_id,
        d_last_name=d_last_name,
        mothra_id=mothra_id,
        student_id=student_id,
        external_id=external_id,
        iam_id=iam_id,
        pps_id=pps_id,
        banner_p_id_m=banner_p_id_m,
        user_id=user_id,
        email=email,
        dept_code=dept_code,
        title_code=title_code,
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
    o_first_name: Union[Unset, str] = UNSET,
    o_middle_name: Union[Unset, str] = UNSET,
    o_last_name: Union[Unset, str] = UNSET,
    d_first_name: Union[Unset, str] = UNSET,
    d_middle_name: Union[Unset, str] = UNSET,
    employee_id: Union[Unset, str] = UNSET,
    d_last_name: Union[Unset, str] = UNSET,
    mothra_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    pps_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    dept_code: Union[Unset, str] = UNSET,
    title_code: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM People Composite

     Kitchen Sink Query, gets almost all the information available for all people requested. Limited
    acces since this query is resource intensive.

    Args:
        o_first_name (Union[Unset, str]):
        o_middle_name (Union[Unset, str]):
        o_last_name (Union[Unset, str]):
        d_first_name (Union[Unset, str]):
        d_middle_name (Union[Unset, str]):
        employee_id (Union[Unset, str]):
        d_last_name (Union[Unset, str]):
        mothra_id (Union[Unset, str]):
        student_id (Union[Unset, str]):
        external_id (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        pps_id (Union[Unset, str]):
        banner_p_id_m (Union[Unset, str]):
        user_id (Union[Unset, str]):
        email (Union[Unset, str]):
        dept_code (Union[Unset, str]):
        title_code (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        o_first_name=o_first_name,
        o_middle_name=o_middle_name,
        o_last_name=o_last_name,
        d_first_name=d_first_name,
        d_middle_name=d_middle_name,
        employee_id=employee_id,
        d_last_name=d_last_name,
        mothra_id=mothra_id,
        student_id=student_id,
        external_id=external_id,
        iam_id=iam_id,
        pps_id=pps_id,
        banner_p_id_m=banner_p_id_m,
        user_id=user_id,
        email=email,
        dept_code=dept_code,
        title_code=title_code,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
