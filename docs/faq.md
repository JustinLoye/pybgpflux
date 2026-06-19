# FAQ

Frequently asked questions about PyBGPFlux.

## Why do I need a library to stream BGP data?

Even basic historical BGP data queries often involve several manual steps: locating the correct archive, downloading it, and parsing the contents. For more complex queries, such as those involving multiple collectors or specific time ranges, the difficulty increases as users have to manually manage and synchronize numerous archive files.

The goal of PyBGPStream and PyBGPFlux is to abstract these complexities away. This allows users to focus on the actual data analysis rather than the infrastructure and boilerplate code required to fetch it.

## Why a new library to stream BGP data?

While PyBGPStream has long been the primary tool for streaming historical BGP data from multiple collectors, it is currently no longer actively maintained. As of early 2026, several key features are lagging (extended communities, RFC 9234) or have become unreliable (broker).

PyBGPFlux was developed to fill this gap, providing a modern, maintained alternative that restores these capabilities.
Rather than relying on simple bindings, PyBGPFlux is a library designed for maximum flexibility and resilience, utilizing Python to seamlessly integrate multiple parsers and brokers.

## What's the difference between PyBGPFlux and PyBGPStream?

PyBGPFlux is a drop-in replacement for PyBGPStream that uses by default BGPKIT for retrieve and parsing MRT files. Key advantages:

- **More flexible**: Multiple parser and broker backends
- **No down time**: BGPKIT broker and BGPFinder can be self-hosted
- **Modern Python**: Type hints, pydantic for configuration

Setting up the stream is different, but the output BGP elements have the same format.

## Can I use this with live data?

Yes! Set both `start_time` and `end_time` to `None` for live mode:

```python
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    collectors=["rrc00"],
    data_types=["updates"],
)

stream = BGPStream.from_config(config)
for elem in stream:
    print(elem)  # Live updates
```

## How do I filter for specific data?

Use `FilterOptions`:

```python
from pybgpflux import FilterOptions

filters = FilterOptions(
    peer_asn=2497,             # Specific AS
    prefix="192.0.2.0/24",     # Specific prefix
    update_type="announce",    # Only announcements
    ip_version=4,              # IPv4 only
)

config = BGPStreamConfig(
    ...,
    filters=filters,
)
```

## Can I combine multiple filters?

Yes, all filters use AND logic:

```python
filters = FilterOptions(
    peer_asn=2497,
    prefix_super="192.0.2.0/24",
    origin_asn=1234,
    ip_version=4,
)
# Returns: IPv4 prefixes originated by 1234,
#          more general than 192.0.2.0/24,
#          with peer 2497
```

## How to cache the MRT archives?

### Cache the Archive Files

Caching and other implementation details are optional parameters on `BGPStreamConfig`:

```python
import datetime
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide", "rrc04"],
    data_types=["updates"],
    cache_dir="cache",
    max_concurrent_downloads=5,
)

stream = BGPStream.from_config(config)
for elem in stream:
    print(elem)
```

## The stream is slow. How do I improve it?

1. **Switch parser**: Use `pybgpstream` or `bgpkit` parser instead of `pybgpkit`

2. **Add filters**: Reduce the dataset

3. **Reduce time window**: Request less data

See [Performance Guide](performance.md) for more tips.

## Can I process very large datasets?

Yes! Process in streaming fashion:

```python
# Good: constant memory
count = 0
for elem in stream:
    count += 1

# Bad: grows linearly with elements
elements = list(stream)  # AVOID for large datasets!
```

## More Help

- **Issues**: [GitHub Issues](https://github.com/JustinLoye/pybgpflux/issues)
- **Discussions**: [GitHub Discussions](https://github.com/JustinLoye/pybgpflux/discussions)
