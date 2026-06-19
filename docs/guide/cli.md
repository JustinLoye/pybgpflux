# CLI Tool

PyBGPFlux includes a command-line interface for quick BGP data exploration.
For a high-performance CLI, check out [bgpflux](https://github.com/JustinLoye/bgpflux).

## Installation

The CLI tool is automatically available after installation:

```bash
pip install pybgpflux
```

## Basic Usage

### Simple Stream

Stream BGP updates from selected collectors:

```bash
pybgpflux \
  --start-time 2010-09-01T00:00:00 \
  --end-time 2010-09-01T02:00:00 \
  --collectors route-views.wide route-views.sydney \
  --data-types updates
```

### Output Format

The CLI outputs elements in pipe-separated format:

```
A|1283289600.000000|route-views.wide|2497|192.0.2.1|192.0.2.0/24|192.0.2.254|2497 2498|None|None|None
```

Fields: `type|time|collector|peer_asn|peer_address|prefix|next-hop|as_path|communities|old_state|new_state`

## Filtering Options

### By Origin AS

```bash
pybgpflux \
  --start-time 2010-09-01T00:00:00 \
  --end-time 2010-09-01T02:00:00 \
  --collectors route-views.wide \
  --peer-asn 2497
```

### By Prefix

```bash
# Exact match
pybgpflux ... --prefix 192.0.2.0/24

# With super-prefixes (more general)
pybgpflux ... --prefix-super 192.0.2.0/25

# With sub-prefixes (more specific)
pybgpflux ... --prefix-sub 192.0.2.0/24

# With both super and sub
pybgpflux ... --prefix-super-sub 192.0.2.0/25
```

### By Peer

```bash
# Single peer IP
pybgpflux ... --peer-ip 192.0.2.1

# Multiple peer IPs
pybgpflux ... --peer-ips 192.0.2.1 192.0.2.2 192.0.2.3

# By peer AS
pybgpflux ... --peer-asn 2497
```

### By Update Type

```bash
# Announcements only
pybgpflux ... --update-type announce

# Withdrawals only
pybgpflux ... --update-type withdraw
```

### By AS Path

```bash
# Regular expression matching
pybgpflux ... --as-path ".*2497.*"
```

### By IP Version

```bash
# IPv4 only
pybgpflux ... --ip-version 4

# IPv6 only
pybgpflux ... --ip-version 6
```

## All Options

```bash
pybgpflux --help
```

Shows all available command-line options with descriptions.

### Next: [API Reference](../api/overview.md)
