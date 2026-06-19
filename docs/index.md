# PyBGPFlux

A drop-in replacement for PyBGPStream using BGPKIT.

## Overview

**PyBGPFlux** is a framework designed for the streaming and analysis of BGP data from multiple RIPE RIS and RouteViews collectors. It utilizes BGPKIT's broker and parser to produce time-ordered messages from RIB and update files, serving as an alternative implementation for workflows previously utilizing PyBGPStream.

## Key Features

- **Drop-in Replacement**: Seamlessly replace PyBGPStream in your existing code
- **High Performance**: Comparable to PyBGPStream
- **Lazy Loading**: Minimal memory consumption suitable for large datasets
- **Multiple Parsers**: Support for `pybgpkit`, `bgpkit-parser`, `bgpdump`, and `pybgpstream` parsers
- **Multiple Brokers**: [`bgpkit`](https://bgpkit.com/tools/broker) (default, self-hostable), [`bgpstream`](https://bgpstream.caida.org/docs/api/broker), [`bgpfinder`](https://bgpfinder.inetintel.cc.gatech.edu) (self-hostable)
- **Caching**: Concurrent downloading with BGPKIT parser caching compatibility
- **Live Streaming**: Real-time BGP message streaming via RIS Live
- **Flexible Filtering**: Filter by ASN, prefix, peer IP, ...
- **CLI Tool**: Command-line interface for quick BGP data exploration
- **Modern Python features:** Type hints and Pydantic validation

## Installation

```bash
pip install pybgpflux
```

## Quick Start

```python
import datetime
from pybgpflux import BGPStreamConfig, BGPStream

# Configure the stream
config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide", "rrc04"],
    data_types=["updates"],
)

# Create stream and iterate
stream = BGPStream.from_config(config)
for elem in stream:
    print(f"{elem.type}|{elem.time}|{elem.collector}|{elem.peer_asn}|{elem.fields['prefix']}")
```

## Motivation

### Why do I need a library to stream BGP data?

Even basic historical BGP data queries often involve several manual steps: locating the correct archive, downloading it, and parsing the contents. For more complex queries, such as those involving multiple collectors or specific time ranges, the difficulty increases as users have to manually manage and synchronize numerous archive files.

The goal of PyBGPFlux is to abstract these complexities away. This allows users to focus on the actual data analysis rather than the infrastructure and boilerplate code required to fetch it.

### Why a new library to stream BGP data?

While PyBGPStream has long been the primary tool for streaming historical BGP data from multiple collectors, it is currently no longer actively maintained. As of early 2026, several key features have become unreliable or non-functional, specifically support for RIPE RIS Live and data from certain RIPE RIS collectors.

PyBGPFlux was developed to fill this gap, providing a modern, maintained alternative that restores these capabilities while offering the performance benefits of the BGPKIT ecosystem.

## What's Next?

- [Getting Started Guide](getting_started.md) - Set up your first BGP stream
- [User Guide](guide/configuration.md) - Learn configuration and filtering options
- [CLI Tool](guide/cli.md) - Use the command-line interface
- [API Reference](api/overview.md) - Explore the full API
- [Performance Guide](performance.md) - Benchmark and optimization tips

## License

MIT