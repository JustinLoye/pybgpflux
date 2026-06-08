# Parser Backends

PyBGPFlux supports multiple parser backends for different use cases.

## Available Parsers

### PyBGPKIT (Default)

- **Name**: `pybgpkit`
- **Speed**: Slow
- **Dependencies**: None
- **Use Case**: Zero-dependency installation, prototyping

```python
stream = BGPStream.from_config(config, parser_name="pybgpkit")
```

### BGPKIT Parser

- **Name**: `bgpkit`
- **Speed**: Fast (~10x faster than pybgpkit)
- **Dependencies**: Install from cargo
- **Use Case**: Large-scale processing

Installation:
```bash
cargo install bgpkit-parser --features cli
```

```python
stream = BGPStream.from_config(config, parser_name="bgpkit")
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

```python
stream = BGPStream.from_config(config, parser_name="bgpdump")
```

### PyBGPStream

- **Name**: `pybgpstream`
- **Speed**: Fastest
- **Remote parsing**: Yes (parse directly from URL, no local download)
- **Dependencies**: `pip install pybgpstream`
- **Use Case**: Large-scale processing, low-disk/RAM environments

Installation: follow the [CI steps](https://github.com/JustinLoye/pybgpflux/blob/main/.github/workflows/ci.yml)

```python
stream = BGPStream.from_config(config, parser_name="pybgpstream")
```

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