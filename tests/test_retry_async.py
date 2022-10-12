from unittest.mock import create_autospec
from unittest.mock import MagicMock, AsyncMock
import time

import pytest

from retry_async.api import retry_call_async
from retry_async.api import retry


@pytest.mark.asyncio
async def test_retry(monkeypatch):
    mock_sleep_time = [0]

    def mock_sleep(seconds):
        mock_sleep_time[0] += seconds

    monkeypatch.setattr(time, "sleep", mock_sleep)

    hit = [0]

    tries = 5
    delay = 1
    backoff = 2

    @retry(is_async=True, tries=tries, delay=delay, backoff=backoff)
    async def f():
        hit[0] += 1
        1 / 0

    with pytest.raises(ZeroDivisionError):
        await f()
    assert hit[0] == tries
    assert mock_sleep_time[0] == sum(delay * backoff ** i for i in range(tries - 1))


@pytest.mark.asyncio
async def test_tries_inf():
    hit = [0]
    target = 10

    @retry(is_async=True, tries=float("inf"))
    async def f():
        hit[0] += 1
        if hit[0] == target:
            return target
        else:
            raise ValueError

    assert await f() == target


@pytest.mark.asyncio
async def test_tries_minus1():
    hit = [0]
    target = 10

    @retry(is_async=True, tries=-1)
    async def f():
        hit[0] += 1
        if hit[0] == target:
            return target
        else:
            raise ValueError

    assert await f() == target


@pytest.mark.asyncio
async def test_max_delay(monkeypatch):
    mock_sleep_time = [0]

    def mock_sleep(seconds):
        mock_sleep_time[0] += seconds

    monkeypatch.setattr(time, "sleep", mock_sleep)

    hit = [0]

    tries = 5
    delay = 1
    backoff = 2
    max_delay = delay  # Never increase delay

    @retry(
        is_async=True, tries=tries, delay=delay, max_delay=max_delay, backoff=backoff
    )
    async def f():
        hit[0] += 1
        1 / 0

    with pytest.raises(ZeroDivisionError):
        await f()
    assert hit[0] == tries
    assert mock_sleep_time[0] == delay * (tries - 1)


@pytest.mark.asyncio
async def test_fixed_jitter(monkeypatch):
    mock_sleep_time = [0]

    def mock_sleep(seconds):
        mock_sleep_time[0] += seconds

    monkeypatch.setattr(time, "sleep", mock_sleep)

    hit = [0]

    tries = 10
    jitter = 1

    @retry(is_async=True, tries=tries, jitter=jitter)
    async def f():
        hit[0] += 1
        1 / 0

    with pytest.raises(ZeroDivisionError):
        await f()
    assert hit[0] == tries
    assert mock_sleep_time[0] == sum(range(tries - 1))


@pytest.mark.asyncio
async def test_retry_call():
    f_mock = MagicMock(side_effect=RuntimeError)
    tries = 2
    try:
        await retry_call_async(f_mock, exceptions=RuntimeError, tries=tries)
    except RuntimeError:
        pass

    assert f_mock.call_count == tries


@pytest.mark.asyncio
async def test_retry_call_2():
    side_effect = [RuntimeError, RuntimeError, 3]
    f_mock = AsyncMock(side_effect=side_effect)
    tries = 5
    result = None
    try:
        result = await retry_call_async(f_mock, exceptions=RuntimeError, tries=tries)
    except RuntimeError:
        pass

    assert result == 3
    assert f_mock.call_count == len(side_effect)


@pytest.mark.asyncio
async def test_retry_call_with_args():
    async def f(value=0):
        if value < 0:
            return value
        else:
            raise RuntimeError

    return_value = -1
    result = None
    f_mock = AsyncMock(spec=f, return_value=return_value)
    try:
        result = await retry_call_async(f_mock, fargs=[return_value])
    except RuntimeError:
        pass

    assert result == return_value
    assert f_mock.call_count == 1


@pytest.mark.asyncio
async def test_retry_call_with_kwargs():
    async def f(value=0):
        if value < 0:
            return value
        else:
            raise RuntimeError

    kwargs = {"value": -1}
    result = None
    f_mock = AsyncMock(spec=f, return_value=kwargs["value"])
    try:
        result = await retry_call_async(f_mock, fkwargs=kwargs)
    except RuntimeError:
        pass

    assert result == kwargs["value"]
    assert f_mock.call_count == 1
