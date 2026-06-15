from typing import NamedTuple, TypedDict, Literal

# Use the functional TypedDict syntax to support hyphenated keys.
# total=False signals that keys may be absent.
ElementFields = TypedDict(
    "ElementFields",
    {
        "next-hop": str,
        "as-path": str,
        "communities": list[str],
        "prefix": str,
        "old-state": str,
        "new-state": str,
    },
    total=False,
)


class BGPElement(NamedTuple):
    """Compatible with pybgpstream.BGPElem"""

    time: float
    type: Literal["R", "A", "W"]
    collector: str
    peer_asn: int
    peer_address: str
    fields: ElementFields

    def __str__(self) -> str:
        """Credit to pybgpstream"""
        communities_val = self.fields.get("communities")

        return "%s|%f|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (
            self.type,
            self.time,
            self.collector,
            self.peer_asn,
            self.peer_address,
            self.fields.get("prefix"),
            self.fields.get("next-hop"),
            self.fields.get("as-path"),
            " ".join(communities_val) if communities_val else None,
            self.fields.get("old-state"),
            self.fields.get("new-state"),
        )

    # Useful for sorting streams
    def __lt__(self, other: "BGPElement") -> bool:  # type: ignore[reportIncompatibleMethodOverride]
        return self.time < other.time

    def __le__(self, other: "BGPElement") -> bool:  # type: ignore[reportIncompatibleMethodOverride]
        return self.time <= other.time
