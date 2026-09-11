# Broker Backends

PyBGPFlux supports multiple broker backends.  

## Available Brokers

### [BGPKIT](https://bgpkit.com/tools/broker) (Default)

- **Name**: `bgpkit`
- **About**: From the BGPKIT project, used in production at Cloudflare
- **Self-hostable**: Yes

### [BGPStream v2](https://bgpstream.caida.org/docs/api/broker)

- **Name**: `bgpstream`
- **About**: From CAIDA, UCSD
- **Self-hostable**: No
- **Note**: New collectors and some archives (e.g., RIS in early 2026) are missing

Usage:
```python
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    ...,
    broker="bgpstream",
)
stream = BGPStream.from_config(config)
```

### [BGPFinder](https://bgpfinder.inetintel.cc.gatech.edu)

- **Name**: `bgpfinder`
- **About**: Georgia Tech's implementation of BGPStream v2 API
- **Self-hostable**: Yes

Usage:
```python
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    ...,
    broker="bgpfinder",
)
stream = BGPStream.from_config(config)
```

## Broker-level query options

Some `BGPStreamConfig` options are enforced by the broker rather than the stream,
which means they also apply when a broker is used standalone:

- `rib_period` — minimum archive-time interval between two RIBs of the same
  collector, see [RIB period](configuration.md#rib-period).

Brokers also guarantee that their results are sorted by ascending archive start
time.

!!! note "BGPStream v2 response limits"

    The `bgpstream` and `bgpfinder` brokers silently cap every response, and the
    cap differs per deployment: CAIDA returns less than two hours of archive data
    whatever interval is requested, while BGPFinder returns around five hundred
    files. PyBGPFlux measures the page size from the first response and tiles the
    rest of the interval with chunks of that size, run concurrently, so a long
    window always returns the complete file list. A two-day window costs three
    requests on BGPFinder and about one per hour of stream on CAIDA.

## Using brokers without streaming data

You can also use brokers independently:

```python
--8<-- "examples/broker_standalone.py"
```

### Next: [Streaming data - handling large datasets](streaming.md)