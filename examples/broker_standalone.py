import datetime

from pybgpflux.brokers import BGPStreamBroker, BGPKITBroker
from pybgpflux import BGPStreamConfig

config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 1),
    end_time=datetime.datetime(2010, 9, 1, 0, 59),
    collectors=["route-views.wide"],
    data_types=["ribs", "updates"],
)

bgpkit_items = BGPKITBroker().query(config)
caida_items = BGPStreamBroker(url="https://broker.bgpstream.caida.org/v2").query(config)

assert len(bgpkit_items) == len(caida_items)
