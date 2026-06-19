# Filtering Data

Apply filters to your BGP stream to focus on specific prefixes, ASes, and peers.

## FilterOptions

The `FilterOptions` class provides comprehensive filtering capabilities. See [API Reference](../api/configuration.md) for full documentation.

## Common Filtering Examples

### Filter by Origin AS

Focus on prefixes originated by a specific AS:

```python
from pybgpflux import BGPStreamConfig, FilterOptions
import datetime

config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide"],
    filters=FilterOptions(origin_asn=2497),
)
```

### Filter by Prefix

Match specific prefixes. Three modes available:

```python
# Exact match
filters = FilterOptions(prefix="192.0.2.0/24")

# Exact match + super-prefixes (more general)
filters = FilterOptions(prefix_super="192.0.2.0/25")

# Exact match + sub-prefixes (more specific)
filters = FilterOptions(prefix_sub="192.0.2.0/24")

# Exact match + both super and sub prefixes
filters = FilterOptions(prefix_super_sub="192.0.2.0/25")
```

### Filter by BGP Peer

```python
# Single peer IP
filters = FilterOptions(peer_ip="192.0.2.1")

# Multiple peer IPs
filters = FilterOptions(peer_ips=["192.0.2.1", "192.0.2.2"])

# Single peer ASN
filters = FilterOptions(peer_asn=2497)
```

### Filter by Update Type

```python
# Only announcements (new/updated prefixes)
filters = FilterOptions(update_type="announce")

# Only withdrawals (removed prefixes)
filters = FilterOptions(update_type="withdraw")
```

### Filter by IP Version

```python
# IPv4 only
filters = FilterOptions(ip_version=4)

# IPv6 only
filters = FilterOptions(ip_version=6)
```

### Filter by AS Path

Use regular expressions to match AS paths:

```python
# Paths containing AS 2497
filters = FilterOptions(as_path=".*2497.*")

# Paths starting with AS 2497
filters = FilterOptions(as_path="^2497.*")

# Paths with AS 2497 directly connected
filters = FilterOptions(as_path=".*2497 [0-9]+.*")
```

Check before that the requested parser supports it.

## Combining Filters

All filters are combined with AND logic:

```python
config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 0),
    end_time=datetime.datetime(2010, 9, 1, 2, 0),
    collectors=["route-views.wide"],
    filters=FilterOptions(
        origin_asn=2497,
        prefix_super="192.0.2.0/24",
        update_type="announce",
        ip_version=4,
    ),
)
```

This will match only IPv4 announcements from AS 2497 for 192.0.2.0/24 and its super-prefixes.

## Performance Tips

- Filters reduce memory and CPU usage by dropping unwanted elements early
- More specific filters (exact AS, prefix) are generally faster
- AS path regex matching can be expensive—keep patterns efficient
- Consider using prefix/peer filters before AS path filters for better performance

### Next: [Choosing a parser](parsers.md)
