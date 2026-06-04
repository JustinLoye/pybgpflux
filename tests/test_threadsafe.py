import datetime
import multiprocessing
import os
import shutil

from pybgpflux import BGPStream, BGPStreamConfig
from tests.pybgpstream_utils import make_bgpstream


SHARED_CACHE = "cache_threadsafe_test"

N_PROCS = 4


def _stream_elem_count(config: BGPStreamConfig):
    """Run BGPStream and return the element count."""
    stream = BGPStream.from_config(config)
    return sum(1 for _ in stream)


def test_threadsafe():
    """Check that concurrent BGPStream don't interfere with each others (especially data race in the cache miss case)"""
    if os.path.exists(SHARED_CACHE):
        shutil.rmtree(SHARED_CACHE)
    os.makedirs(SHARED_CACHE)

    config = BGPStreamConfig(
        start_time=datetime.datetime(2010, 9, 1, 0, 0),
        end_time=datetime.datetime(2010, 9, 1, 1, 0),
        collectors=["route-views.wide"],
        data_types=["updates"],
        cache_dir=SHARED_CACHE,
    )

    with multiprocessing.Pool(N_PROCS) as pool:
        results = pool.map(_stream_elem_count, [config] * N_PROCS)

    for i in range(N_PROCS):
        assert results[i] > 0, f"Process {i} produced no elements"

    assert len(set(results)) == 1, "Count mismatch between processes"

    # Validate against pybgpstream reference
    ref_stream = make_bgpstream(config)
    ref_count = sum(1 for _ in ref_stream)
    assert results[0] == ref_count, (
        f"pybgpflux ({results[0]}) != pybgpstream ({ref_count})"
    )
    shutil.rmtree(SHARED_CACHE)
