# Streaming Data

Understand how to stream and process BGP data efficiently.

## Basic Iteration

The simplest way to process BGP data:

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
    print(f"{elem.type}|{elem.time}|{elem.collector}|{elem.peer_asn}|{elem.fields['prefix']}")
```

## BGPElement Structure

Each element in the stream is a `BGPElement` namedtuple:

::: pybgpflux.bgpelement.BGPElement
    options:
      show_source: false

### Element Fields (Dictionary)

The `fields` attribute contains message-specific data:

```python
elem.fields = {
    "prefix": "192.0.2.0/24",
    "next-hop": "192.0.2.1",
    "as-path": "2497 2498 2499",
    "communities": ["2497:100", "2498:200"],
}
```

## Element Types

Three types of BGP messages are available:

- **R (RIB)**: Complete routing table snapshot
- **A (Announce)**: New or updated prefix announcement
- **W (Withdraw)**: Prefix withdrawal

```python
for elem in stream:
    if elem.type == "R":
        print(f"RIB: {elem.prefix} → {elem.fields['next-hop']}")
    elif elem.type == "A":
        print(f"Announce: {elem.prefix}")
    elif elem.type == "W":
        print(f"Withdraw: {elem.prefix}")
```

## Live Streaming

Stream real-time BGP updates from RIS Live:

```python
from pybgpflux import BGPStreamConfig, BGPStream

# Live mode: no start_time or end_time
config = BGPStreamConfig(
    collectors=["rrc00", "rrc01"],
    data_types=["updates"],
)

stream = BGPStream.from_config(config)

print("Streaming live BGP updates...")
for elem in stream:
    print(f"[{elem.collector}] {elem.type}: {elem.prefix}")
```

## Memory Efficiency

PyBGPFlux uses lazy loading to minimize memory usage:

- Elements are parsed on-demand, not loaded into memory upfront
- Archive files are prefetched only one ahead of time

### Next: [CLI Tool](cli.md)
