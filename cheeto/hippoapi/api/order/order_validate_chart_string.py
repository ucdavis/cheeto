from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.chart_string_validation_model import ChartStringValidationModel
from ...types import Response


def _get_kwargs(
    chart_string: str,
    direction: str,
) -> Dict[str, Any]:
    _kwargs: Dict[str, Any] = {
        "method": "get",
        "url": "/api/order/validateChartString/{chart_string}/{direction}".format(
            chart_string=chart_string,
            direction=direction,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[ChartStringValidationModel]:
    if response.status_code == HTTPStatus.OK:
        response_200 = ChartStringValidationModel.from_dict(response.json())

        return response_200
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[ChartStringValidationModel]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    chart_string: str,
    direction: str,
    *,
    client: AuthenticatedClient,
) -> Response[ChartStringValidationModel]:
    """
    Args:
        chart_string (str):
        direction (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChartStringValidationModel]
    """

    kwargs = _get_kwargs(
        chart_string=chart_string,
        direction=direction,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    chart_string: str,
    direction: str,
    *,
    client: AuthenticatedClient,
) -> Optional[ChartStringValidationModel]:
    """
    Args:
        chart_string (str):
        direction (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChartStringValidationModel
    """

    return sync_detailed(
        chart_string=chart_string,
        direction=direction,
        client=client,
    ).parsed


async def asyncio_detailed(
    chart_string: str,
    direction: str,
    *,
    client: AuthenticatedClient,
) -> Response[ChartStringValidationModel]:
    """
    Args:
        chart_string (str):
        direction (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ChartStringValidationModel]
    """

    kwargs = _get_kwargs(
        chart_string=chart_string,
        direction=direction,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    chart_string: str,
    direction: str,
    *,
    client: AuthenticatedClient,
) -> Optional[ChartStringValidationModel]:
    """
    Args:
        chart_string (str):
        direction (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ChartStringValidationModel
    """

    return (
        await asyncio_detailed(
            chart_string=chart_string,
            direction=direction,
            client=client,
        )
    ).parsed
