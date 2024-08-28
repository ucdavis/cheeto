from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    employee_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}

    params["employeeId"] = employee_id

    params["studentId"] = student_id

    params["externalId"] = external_id

    params["iamId"] = iam_id

    params["bannerPIdM"] = banner_p_id_m

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: Dict[str, Any] = {
        "method": "get",
        "url": "/iam/people/genderidentity/search",
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
    employee_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM People gender identity Search

     This API will need a special key that will be granted after an approval before a user can call it
     Search for people in IAM database

    Args:
        employee_id (Union[Unset, str]):
        student_id (Union[Unset, str]):
        external_id (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        banner_p_id_m (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        employee_id=employee_id,
        student_id=student_id,
        external_id=external_id,
        iam_id=iam_id,
        banner_p_id_m=banner_p_id_m,
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
    employee_id: Union[Unset, str] = UNSET,
    student_id: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    banner_p_id_m: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM People gender identity Search

     This API will need a special key that will be granted after an approval before a user can call it
     Search for people in IAM database

    Args:
        employee_id (Union[Unset, str]):
        student_id (Union[Unset, str]):
        external_id (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        banner_p_id_m (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        employee_id=employee_id,
        student_id=student_id,
        external_id=external_id,
        iam_id=iam_id,
        banner_p_id_m=banner_p_id_m,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
