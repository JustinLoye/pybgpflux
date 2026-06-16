import pybgpstream
from pybgpflux import BGPStreamConfig
from pybgpflux.parsers.pybgpstream import generate_bgpstream_filters


def make_bgpstream(
    config: BGPStreamConfig,
) -> pybgpstream.BGPStream:
    stream = pybgpstream.BGPStream(
        from_time=config.start_time.isoformat(" "),
        until_time=config.end_time.isoformat(" "),
        collectors=config.collectors,
        record_types=config.data_types,
        filter=generate_bgpstream_filters(config.filters),
    )
    if config.cache_dir:
        stream.set_data_interface_option(
            "broker", "cache-dir", str(config.cache_dir)
        )

    return stream
