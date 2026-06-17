import bgpkit  # pyright: ignore[reportMissingTypeStubs]
from bgpkit.bgpkit_broker import BrokerItem  # pyright: ignore[reportMissingTypeStubs]

from pybgpflux.bgpstreamconfig import BGPStreamConfig
from pybgpflux.brokers.bgpbroker import BGPBroker, BrokerQueryError


class BGPKITBroker(BGPBroker):
    def query(self, config: BGPStreamConfig) -> list[BrokerItem]:
        broker = bgpkit.Broker()
        items: list[BrokerItem] = []

        try:
            for data_type in config.data_types:
                items.extend(
                    broker.query(  # type: ignore
                        ts_start=str(int(config.start_time.timestamp() - 60)),  # type: ignore
                        ts_end=str(int(config.end_time.timestamp())),  # type: ignore
                        collector_id=",".join(config.collectors),
                        data_type=data_type[:-1],  # removes plural form
                    )
                )
        except Exception as exc:
            raise BrokerQueryError(
                f"BGPKIT Broker query execution failed: {exc}"
            ) from exc

        if not items:
            raise BrokerQueryError(
                "No archives returned from the BGPKIT broker for the given config."
            )

        # Add plural form to ribs to match bgpstream this project
        for item in items:
            if item.data_type == "rib":
                item.data_type = "ribs"
        return items
