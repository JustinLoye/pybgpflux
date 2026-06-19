# Broker Backends

PyBGPFlux supports multiple broker backends.  

## Available Brokers

### [BGPKIT](https://bgpkit.com/tools/broker) (Default)

- **Name**: `bgpkit`
- **About**: From the BGPKIT project, used in production at Cloudflare
- **Self-hostable**: Yes

### [BGPStream v2](https://bgpstream.caida.org/docs/api/broker)

- **Name**: `bgpstream`
- **About**: From CAIDA, UCSD
- **Self-hostable**: No
- **Note**: New collectors and some archives (e.g., RIS in early 2026) are missing

Usage:
```python
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    ...,
    broker="bgpstream",
)
stream = BGPStream.from_config(config)
```

### [BGPFinder](https://bgpfinder.inetintel.cc.gatech.edu)

- **Name**: `bgpfinder`
- **About**: Georgia Tech's implementation of BGPStream v2 API
- **Self-hostable**: Yes

Usage:
```python
from pybgpflux import BGPStreamConfig, BGPStream

config = BGPStreamConfig(
    ...,
    broker="bgpfinder",
)
stream = BGPStream.from_config(config)
```

## Using brokers without streaming data

You can also use brokers independently:

```python
--8<-- "examples/broker_standalone.py"
```

### Next: [Streaming data - handling large datasets](streaming.md)