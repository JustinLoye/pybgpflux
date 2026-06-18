import time
import pytest
from itertools import pairwise

from pybgpflux import LiveStreamConfig, BGPStream


@pytest.fixture
def rislive_config():
    return LiveStreamConfig(
        collectors=["rrc00", "rrc21"], jitter_buffer_delay=25.0
    )


def test_rislive(rislive_config):
    stream = BGPStream.from_config(rislive_config)

    rcs = set()
    times = []
    start = time.time()
    for i, elem in enumerate(stream):
        times.append(elem.time)
        rcs.add(elem.collector)
        if time.time() - start > 30:
            break

    assert i > 0
    assert all(collector in rcs for collector in rislive_config.collectors)
    assert all(t1 <= t2 for t1, t2 in pairwise(times))
