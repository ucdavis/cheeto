from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    iam_id: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    is_faculty: Union[Unset, bool] = UNSET,
    is_staff: Union[Unset, bool] = UNSET,
    is_student: Union[Unset, bool] = UNSET,
    is_external: Union[Unset, bool] = UNSET,
    last_modified: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["iamId"] = iam_id

    params["userId"] = user_id

    params["email"] = email

    params["isFaculty"] = is_faculty

    params["isStaff"] = is_staff

    params["isStudent"] = is_student

    params["isExternal"] = is_external

    params["lastModified"] = last_modified

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/iam/people/profile/search",
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
    iam_id: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    is_faculty: Union[Unset, bool] = UNSET,
    is_staff: Union[Unset, bool] = UNSET,
    is_student: Union[Unset, bool] = UNSET,
    is_external: Union[Unset, bool] = UNSET,
    last_modified: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """Special Search for the Profile application

     Special Search for Profile application

    Args:
        iam_id (Union[Unset, str]):
        user_id (Union[Unset, str]):
        email (Union[Unset, str]):
        is_faculty (Union[Unset, bool]):
        is_staff (Union[Unset, bool]):
        is_student (Union[Unset, bool]):
        is_external (Union[Unset, bool]):
        last_modified (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        iam_id=iam_id,
        user_id=user_id,
        email=email,
        is_faculty=is_faculty,
        is_staff=is_staff,
        is_student=is_student,
        is_external=is_external,
        last_modified=last_modified,
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
    iam_id: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    is_faculty: Union[Unset, bool] = UNSET,
    is_staff: Union[Unset, bool] = UNSET,
    is_student: Union[Unset, bool] = UNSET,
    is_external: Union[Unset, bool] = UNSET,
    last_modified: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """Special Search for the Profile application

     Special Search for Profile application

    Args:
        iam_id (Union[Unset, str]):
        user_id (Union[Unset, str]):
        email (Union[Unset, str]):
        is_faculty (Union[Unset, bool]):
        is_staff (Union[Unset, bool]):
        is_student (Union[Unset, bool]):
        is_external (Union[Unset, bool]):
        last_modified (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        iam_id=iam_id,
        user_id=user_id,
        email=email,
        is_faculty=is_faculty,
        is_staff=is_staff,
        is_student=is_student,
        is_external=is_external,
        last_modified=last_modified,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
