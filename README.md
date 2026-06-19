# PyBGPFlux

[![Docs](https://img.shields.io/badge/docs-justinloye.github.io-blue)](https://justinloye.github.io/pybgpflux/)
[![PyPI - Version](https://img.shields.io/pypi/v/pybgpflux.svg)](https://pypi.org/project/pybgpflux)
[![CI](https://github.com/JustinLoye/pybgpflux/actions/workflows/ci.yml/badge.svg)](https://github.com/JustinLoye/pybgpflux/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/JustinLoye/pybgpflux.svg)](https://github.com/JustinLoye/pybgpflux/blob/main/LICENSE)

A drop-in replacement for PyBGPStream using BGPKIT.  
For a high-performance Rust library and CLI, check out [bgpflux](https://github.com/JustinLoye/bgpflux).

## Features

- Generates time-ordered BGP messages on the fly from RIBs and updates MRT files of multiple collectors
- Stream the same BGP messages as PyBGPStream, enabling seamless, drop-in replacement
- Lazy loading consumes minimal memory, making it suitable for large datasets
- Multiple BGP parsers supported: `pybgpkit` (default but slow), `bgpkit-parser`, `bgpdump` and `pybgpstream` single file backend (the latter three are system dependencies)
- Multiple BGP archive brokers supported: [`bgpkit`](https://bgpkit.com/tools/broker) (default, self-hostable), [`bgpstream`](https://bgpstream.caida.org/docs/api/broker), [`bgpfinder`](https://bgpfinder.inetintel.cc.gatech.edu) (self-hostable)
- Caching with concurrent downloading fully compatible with the BGPKIT parser's caching functionality.
- Performance: for updates, typically 3–10× faster than PyBGPStream; for RIB-only processing, currently about 3–4× slower (see [perf.md](perf.md) for test details).
- A CLI tool

## Quick start

Installation:

```sh
pip install pybgpflux
```

Usage:

```python
import datetime
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 1, 59),
    collectors=["route-views.wide", "rrc04"],
    data_types=["ribs", "updates"],
)

stream = BGPStream.from_config(config)

n_elems = 0
for elem in stream:
  n_elems += 1
    
print(f"Processed {n_elems} BGP elements")
```

or in the terminal:

```sh
pybgpflux --start-time 2010-09-01T00:00:00 --end-time 2010-09-01T01:59:00 --collectors route-views.sydney route-views.wide --data-types updates > updates.txt
```

## Motivation

While PyBGPStream has long been the primary tool for streaming historical BGP data from multiple collectors, it is currently no longer actively maintained. New features are lagging (extended communities, RFC 9234) and the broker has become unreliable.

PyBGPFlux was developed to fill this gap, providing a modern, maintained alternative that restores these capabilities.
PyBGPFlux is a library designed for maximum flexibility, utilizing Python to seamlessly integrate multiple parsers and brokers.

## Missing features

- Live mode for RouteViews collectors
- Some PyBGPStream data interface options like csv or sqlite
