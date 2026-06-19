# Performance Guide

Tips and benchmarks for optimal PyBGPFlux performance.

## Performance Overview

PyBGPFlux achieves performance on par with PyBGPStream:

- **Updates**: 3–10× faster than PyBGPStream
- **RIBs**: Currently 3–4× slower
- **Memory**: Minimal due to lazy loading

For detailed benchmark results, see [perf.md](https://github.com/JustinLoye/pybgpflux/blob/main/perf.md) in the repository.

## Parser Selection

The biggest performance factor is parser choice:

```python
# Fast: bgpkit-parser
stream = BGPStream.from_config(config, parser_name="bgpkit")

# Slow but no dependencies: pybgpkit (baseline)
stream = BGPStream.from_config(config, parser_name="pybgpkit")

# Fast: bgpdump
stream = BGPStream.from_config(config, parser_name="bgpdump")
```

**Recommendation**: Install `bgpkit-parser`.

## Filtering for Performance

Applying filters reduces data processed and improves speed:

```python
# Original: processes all elements
stream = BGPStream(
    collectors=["route-views.wide"],
    data_type=["updates"],
    ts_start=1283203200,
    ts_end=1283289600,
)

# Filtered: processes fewer elements
stream = BGPStream(
    collectors=["route-views.wide"],
    data_type=["updates"],
    ts_start=1283203200,
    ts_end=1283289600,
    filters=FilterOptions(origin_asn=2497),  # Reduces dataset
)
```

## RAM Disk Usage

Use RAM disk (if available) for temporary file storage:

```python
# Automatic: uses /dev/shm (Linux) or /Volumes/RAMDisk (macOS)
stream = BGPStream(..., ram_fetch=True)

# Disable to use system temp directory
stream = BGPStream(..., ram_fetch=False)
```

**Performance benefit**: faster I/O and less disk write on systems with sufficient free RAM.

## Caching Strategy

Reuse cached files to avoid re-downloading:

```python
# Use persistent cache
stream1 = BGPStream(
    ...,
    cache_dir="/data/bgp_cache",
)

# Later: same data is reused from cache
stream2 = BGPStream(
    ...,
    cache_dir="/data/bgp_cache",
)
```

## Remote Parsing

When using the `pybgpstream` parser, enable remote parsing to skip writing to disk entirely (files are parsed directly from their remote URLs):

```python
# No local copy written at all (default when parser="pybgpstream")
stream = BGPStream(..., parser_name="pybgpstream", remote_parse=True)
```

This is the lowest-latency option for one-off queries: it avoids both disk and RAM writes. However the throughput (BGP elems per second) is generally lower. Not available for other parsers.

## Memory Optimization

For very large datasets, minimize memory usage:

```python
# Process in streaming fashion, not storing all elements
element_count = 0
for elem in stream:
    # Process element immediately
    process(elem)
    element_count += 1
    
    # Don't accumulate elements in lists
    # elements.append(elem)  # AVOID!

print(f"Processed {element_count} elements")
```

**Benefit**: Constant memory regardless of dataset size.

## Troubleshooting Performance

### High Memory Usage

1. Set `ram_fetch=False` to use disk instead of RAM
2. Process elements in streaming fashion, don't accumulate

### Slow Parsing

1. Switch the parser from the default `pybgpkit`
2. Use more specific filters to reduce parsing load
3. Don't use remote parsing

## Further Reading

- [Parser Backends Guide](guide/parsers.md)
- [Filtering Guide](guide/filtering.md)
- [Configuration Guide](guide/configuration.md)
