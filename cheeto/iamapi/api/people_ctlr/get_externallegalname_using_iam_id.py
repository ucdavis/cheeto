from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    iam_id: str,
    *,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/iam/people/legalname/externals/{iam_id}".format(
            iam_id=iam_id,
        ),
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
    iam_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM People legalname

     This API will need a special key that will be granted after an approval before a user can call it
     Provides basic person information about the requested person/people

    Args:
        iam_id (str):
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
        key=key,
        v=v,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


async def asyncio_detailed(
    iam_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM People legalname

     This API will need a special key that will be granted after an approval before a user can call it
     Provides basic person information about the requested person/people

    Args:
        iam_id (str):
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
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
