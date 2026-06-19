# API Reference

Complete Python API documentation for PyBGPFlux.

## Core Classes

### [BGPStream](bgpstream.md)

The main class for creating and iterating through BGP streams.

```python
from pybgpflux import BGPStreamConfig, BGPStream
import datetime

config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide"],
)

stream = BGPStream.from_config(config)
for elem in stream:
    print(elem)
```

### [Configuration Classes](configuration.md)

- **BGPStreamConfig**: Main configuration for historical BGP streams
- **LiveStreamConfig**: Configuration for live RIS Live streams
- **FilterOptions**: Filtering options for BGP elements

```python
from pybgpflux import BGPStreamConfig, FilterOptions

config = BGPStreamConfig(
    start_time=...,
    end_time=...,
    collectors=["route-views.wide"],
    filters=FilterOptions(peer_asn=2497),
)
```

### [BGPElement](bgpelement.md)

The data structure representing individual BGP messages.

```python
# Access element data
elem.type        # "R", "A", or "W"
elem.time        # Unix timestamp
elem.collector   # Collector name
elem.peer_asn    # BGP peer AS number
elem.peer_address # BGP peer IP
elem.fields      # Dictionary with prefix, as-path, etc.
```

## Module Structure

```
pybgpflux/
├── __init__.py              # Main exports
├── bgpstream.py             # BGPStream class
├── bgpstreamconfig.py       # Configuration classes
├── bgpelement.py            # BGPElement NamedTuple
├── broker                   # Broker implementations
├── parser                   # Parser implementations
├── rislive.py               # RIS Live streaming
├── utils.py                 # Utility functions
└── cli.py                   # CLI interface
```

## Quick Links

- [Configuration Guide →](../guide/configuration.md)
- [Filtering Guide →](../guide/filtering.md)
- [Streaming Guide →](../guide/streaming.md)
- [CLI Tool →](../guide/cli.md)
