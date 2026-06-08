# Configuration Guide

`BGPStreamConfig` is the single configuration model for PyBGPFlux. It combines both the query specification (what data to retrieve) and optional implementation parameters (how to retrieve it).

See [API Reference](../api/configuration.md#bgpstreamconfig) for full documentation.

## Creating Streams

```python
from pybgpflux import BGPStreamConfig, BGPStream
import datetime

config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide"],
    data_types=["updates"],
)

stream = BGPStream.from_config(config)
for elem in stream:
    print(elem)
```

### With Implementation Options

For control over caching, parser selection, and download concurrency, pass the optional parameters directly:

```python
from pybgpflux import BGPStreamConfig, BGPStream
import datetime

config = BGPStreamConfig(
    # Query parameters
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide"],
    data_types=["updates"],
    # Implementation parameters (all optional)
    parser="bgpkit",
    max_concurrent_downloads=20,
    cache_dir="/tmp/bgp_cache",
    ram_fetch=True,
)

stream = BGPStream.from_config(config)
for elem in stream:
    print(elem)
```

## Query Parameters

### Time-Based Selection

```python
import datetime
from pybgpflux import BGPStreamConfig

# Specify exact time range
config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0, tzinfo=datetime.timezone.utc),
    end_time=datetime.datetime(2010, 9, 1, 2, 0, tzinfo=datetime.timezone.utc),
    collectors=["route-views.wide"],
)
```

**Important**:

- Datetimes are assumed to be UTC if no timezone is specified
- Both `start_time` and `end_time` must be provided together, or both left as `None`
- Leaving both times as `None` enables live mode

### Collector Selection

```python
# Single or multiple collectors
config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide", "route-views.sydney", "rrc04"],
)
```

Common collectors:

- `route-views.*` - Route Views collectors
- `rrc0*` - RIPE NCC RIS collectors
- Full list available via BGPKIT Broker API

### Data Types

```python
# RIBs: Complete routing table snapshots
config = BGPStreamConfig(
    ...,
    data_types=["ribs"],
)

# Updates: BGP update messages
config = BGPStreamConfig(
    ...,
    data_types=["updates"],
)

# Both
config = BGPStreamConfig(
    ...,
    data_types=["ribs", "updates"],
)
```

## Implementation Parameters

These optional parameters control how PyBGPFlux retrieves and processes data:

### Parser Selection

```python
from pybgpflux import BGPStreamConfig
import datetime

# Default: pybgpkit (pure Python, slower but no dependencies)
config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide"],
    parser="pybgpkit",
)

# bgpkit-parser (requires system install, fastest)
config = BGPStreamConfig(
    ...,
    parser="bgpkit",
)

# bgpdump (requires system install)
config = BGPStreamConfig(
    ...,
    parser="bgpdump",
)

# pybgpstream (requires pip install pybgpstream)
config = BGPStreamConfig(
    ...,
    parser="pybgpstream",
)
```

### Caching and Download Strategy

```python
from pybgpflux import BGPStreamConfig

config = BGPStreamConfig(
    ...,
    cache_dir="/tmp/bgp_cache",           # Directory for downloaded files
    ram_fetch=True,                        # Use /dev/shm (Linux) or /Volumes/RAMDisk (macOS) when cache is disabled
    max_concurrent_downloads=10,           # Number of parallel downloads
)
```

**Parameter details:**

- `cache_dir`: Persistent storage for MRT files. Reused across runs. Setting this disables remote parsing.
- `ram_fetch`: When caching is disabled, use shared memory instead of disk temp space. Improves performance at higher RAM cost.
- `max_concurrent_downloads`: Balance between download speed and resource consumption.

### Remote Parsing

When using the `pybgpstream` parser, PyBGPFlux can parse MRT archives directly from their remote URLs without saving them to disk. This is the default behaviour and requires no extra configuration:

```python
config = BGPStreamConfig(
    ...,
    parser="pybgpstream",
    remote_parse=True,   # default — skip local download entirely
)
```

Remote parsing is automatically disabled when `cache_dir` is set. To force local downloading even without a cache directory, set `remote_parse=False`.

## Direct BGPStream Constructor

It's possible to bypass Pydantic config validation by instantiating `BGPStream` directly:

```python
from pybgpflux import BGPStream

stream = BGPStream(
    collectors=["route-views.wide"],
    data_type=["updates"],
    ts_start=1283203200,  # Unix timestamp
    ts_end=1283289600,    # Unix timestamp
    parser_name="bgpkit",
    max_concurrent_downloads=10,
    cache_dir="/path/to/cache",
    ram_fetch=True,
)
```

### Next: [Filtering Data](filtering.md)
