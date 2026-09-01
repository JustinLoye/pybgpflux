import datetime

from pybgpflux.brokers import BGPStreamBroker, BGPKITBroker
from pybgpflux import BGPStreamConfig

config = BGPStreamConfig(
    start_time=datetime.datetime(2010, 9, 1, 0, 1),
    end_time=datetime.datetime(2010, 9, 7, 0, 59),
    collectors=["route-views.wide", "rrc00", "route-views.linx", "rrc02", "rrc01", "rrc04", "rrc07", "route-views.amsix", "route-views.sydney", "route-views3"],
    data_types=["ribs", "updates"],
)

bgpkit_items = BGPKITBroker().query(config)
caida_items = BGPStreamBroker(url="https://broker.bgpstream.caida.org/v2").query(config)

print(len(bgpkit_items), len(caida_items))
assert len(bgpkit_items) == len(caida_items)
