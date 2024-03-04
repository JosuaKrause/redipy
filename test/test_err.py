# Copyright 2024 Josua Krause
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests various error cases."""

from test.util import get_setup

import pytest

from redipy.api import as_key_type
from redipy.main import Redis


def test_errors() -> None:
    """Tests various error or edge cases."""
    with pytest.raises(ValueError, match="unknown key type: foo"):
        as_key_type("foo")
    assert as_key_type("none") is None

    with pytest.raises(ValueError, match="unknown backend foo"):
        Redis(backend="foo")  # type: ignore

    rt_redis = Redis("custom", rt=get_setup("test_err", rt_lua=True))
    assert rt_redis.maybe_get_redis_runtime() is not None
    assert rt_redis.get_redis_runtime() is not None
    assert rt_redis.maybe_get_memory_runtime() is None
    with pytest.raises(ValueError, match="not a memory runtime"):
        rt_redis.get_memory_runtime()

    rt_mem = Redis("custom", rt=get_setup("test_err", rt_lua=False))
    assert rt_mem.maybe_get_redis_runtime() is None
    with pytest.raises(ValueError, match="not a redis runtime"):
        assert rt_mem.get_redis_runtime() is not None
    assert rt_mem.maybe_get_memory_runtime() is not None
    assert rt_mem.get_memory_runtime() is not None
