import argparse
import sys
import datetime

from pybgpflux import (
    BGPStreamConfig,
    FilterOptions,
    BGPStream,
)


def main():
    parser = argparse.ArgumentParser(
        description="Stream and filter BGP data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Arguments with default values for BGPStreamConfig
    parser.add_argument(
        "--start-time",
        type=datetime.datetime.fromisoformat,
        default=datetime.datetime(2010, 9, 1, 0, 0),
        help="Start of the stream in ISO format.",
    )
    parser.add_argument(
        "--end-time",
        type=datetime.datetime.fromisoformat,
        default=datetime.datetime(2010, 9, 1, 2, 0),
        help="End of the stream in ISO format.",
    )
    parser.add_argument(
        "--collectors",
        type=str,
        nargs="+",
        default=["route-views.sydney", "route-views.wide"],
        help="List of collectors to get data from.",
    )
    parser.add_argument(
        "--data-types",
        type=str,
        nargs="+",
        choices=["ribs", "updates"],
        default=["updates"],
        help="List of archives to consider ('ribs' or 'updates').",
    )

    parser.add_argument(
        "--rib-period",
        type=float,
        default=None,
        help=(
            "Minimum number of seconds between two RIBs of the same collector. "
            "0 or negative processes only the first RIB of each collector."
        ),
    )

    # Arguments for FilterOptions
    parser.add_argument(
        "--origin-asn",
        type=int,
        default=None,
        help="Filter by the origin AS number.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default=None,
        help="Filter by an exact prefix match.",
    )
    parser.add_argument(
        "--prefix-super",
        type=str,
        default=None,
        help="Filter by the exact prefix and its more general super-prefixes.",
    )
    parser.add_argument(
        "--prefix-sub",
        type=str,
        default=None,
        help="Filter by the exact prefix and its more specific sub-prefixes.",
    )
    parser.add_argument(
        "--prefix-super-sub",
        type=str,
        default=None,
        help="Filter by the exact prefix and both its super- and sub-prefixes.",
    )
    parser.add_argument(
        "--peer-ip",
        type=str,  # Note: argparse does not directly handle Union types, so we use string here.
        default=None,
        help="Filter by the IP address of a single BGP peer.",
    )
    parser.add_argument(
        "--peer-ips",
        type=str,
        nargs="+",
        default=None,
        help="Filter by a list of BGP peer IP addresses.",
    )
    parser.add_argument(
        "--peer-asn",
        type=int,
        default=None,
        help="Filter by the AS number of the BGP peer.",
    )
    parser.add_argument(
        "--update-type",
        type=str,
        choices=["withdraw", "announce"],
        default=None,
        help="Filter by the BGP update message type.",
    )
    parser.add_argument(
        "--as-path",
        type=str,
        default=None,
        help="Filter by a regular expression matching the AS path.",
    )

    # PyBGPStream implementation parameters
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=None,
        help="Directory for caching downloaded files.",
    )
    parser.add_argument(
        "--parser",
        type=str,
        choices=["pybgpkit", "bgpkit", "pybgpstream", "bgpdump"],
        default="pybgpkit",
    )

    args = parser.parse_args()

    filter_options = FilterOptions(
        origin_asn=args.origin_asn,
        prefix=args.prefix,
        prefix_super=args.prefix_super,
        prefix_sub=args.prefix_sub,
        prefix_super_sub=args.prefix_super_sub,
        peer_ip=args.peer_ip,
        peer_ips=args.peer_ips,
        peer_asn=args.peer_asn,
        update_type=args.update_type,
        as_path=args.as_path,
    )

    # Convert filter to None if all filter attributes are None
    if all(value is None for value in filter_options.model_dump().values()):
        filter_options = None

    config = BGPStreamConfig(
        start_time=args.start_time,
        end_time=args.end_time,
        collectors=args.collectors,
        data_types=args.data_types,
        filters=filter_options,
        rib_period=datetime.timedelta(seconds=args.rib_period)
        if args.rib_period is not None
        else None,
        cache_dir=args.cache_dir,
        parser=args.parser,
    )

    try:
        for element in BGPStream.from_config(config):
            print(element)
    except Exception as e:
        print(f"An error occurred during streaming: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
