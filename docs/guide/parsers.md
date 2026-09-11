# Parser Backends

PyBGPFlux supports multiple parser backends for different use cases.

## Available Parsers

### PyBGPKIT (Default)

- **Name**: `pybgpkit`
- **Speed**: Slow
- **Dependencies**: None
- **Use Case**: Zero-dependency installation, prototyping

### BGPKIT Parser

- **Name**: `bgpkit`
- **Speed**: Fast (~10x faster than pybgpkit)
- **Dependencies**: Install from cargo
- **Use Case**: Large-scale processing

Installation:
```bash
cargo install bgpkit-parser --features cli
```

Usage:
```python
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    ...,
    parser="bgpkit",
)
stream = BGPStream.from_config(config)
```

### BGPDump

- **Name**: `bgpdump`
- **Speed**: Fast, comparable to BGPKIT
- **Dependencies**: Classic MRT parser utility
- **Use Case**: Legacy systems, compatibility

Installation:
```bash
apt-get install bgpdump
```

### PyBGPStream

- **Name**: `pybgpstream`
- **Speed**: Fastest
- **Remote parsing**: Yes (parse directly from URL, no local download)
- **Dependencies**: libbgpstream and `pip install pybgpstream`
- **Use Case**: Large-scale processing, low-disk/RAM environments

Installation: follow the [CI steps](https://github.com/JustinLoye/pybgpflux/blob/main/.github/workflows/ci.yml)

When `remote_parse=True` (the default), `pybgpstream` parses MRT archives directly from their remote URLs without saving them to disk or RAM first. This is useful for memory-constrained environments or when processing large RIB files.

```python
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    ...,
    parser="pybgpstream",
    remote_parse=True,   # default — skip local download entirely
)
stream = BGPStream.from_config(config)
```

## Using parsers on a single file

You can also use parsers independently. The example below reads a local MRT file;
download one from a collector archive first, for instance
[updates.20100901.0000.bz2](http://archive.routeviews.org/route-views.wide/bgpdata/2010.09/UPDATES/updates.20100901.0000.bz2),
and save it as `data/updates.20100901.0000.bz2`.

```python
--8<-- "examples/parser_standalone.py"
```

### Next: [Available brokers](brokers.md)
