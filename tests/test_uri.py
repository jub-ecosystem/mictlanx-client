# tests/test_mictlanx_uri.py
import pytest

# EDIT ME: point this to where your class lives.
# e.g. if you saved the class in mictlanx/uri.py:
from mictlanx.utils.uri import MictlanXURI
from mictlanx.services import AsyncRouter,AsyncPeer


def assert_router(r: AsyncRouter, *, rid, host, port, protocol="http", http2=False, api_version=4):
    assert r.router_id == rid
    assert r.ip_addr == host
    assert r.port == port
    assert r.protocol == protocol
    assert r.http2 == http2
    assert r.api_version == api_version

def test_parse_domain():
    uri = "mictlanx://r0@localhost:60666,apix.tamps.cinvestav.mx/mictlanxx:60667/?protocol=https&api_version=4&http2=0"
    routers = MictlanXURI.parse(uri)
    assert len(routers) == 2
    for r in routers:
        print(r,r.protocol)
    # print(routers)


def test_parse_canonical_two_routers():
    uri = "mictlanx://r0@localhost:60666,r1@localhost:60667/?protocol=http&api_version=4&http2=0"
    routers = MictlanXURI.parse(uri)
    assert len(routers) == 2
    assert_router(routers[0], rid="r0", host="localhost", port=60666, protocol="http", http2=False, api_version=4)
    assert_router(routers[1], rid="r1", host="localhost", port=60667, protocol="http", http2=False, api_version=4)


def test_parse_original_nonstandard_form_normalizes():
    # Your original example (with ::// and the /...?... query style)
    uri = "mictlanx://mictlanx-router-0@localhost:60666,mictlanx-router-1@localhost:60667/?protocol=http&api_version=4&http2=0"
    routers = MictlanXURI.parse(uri)
    assert len(routers) == 2
    assert_router(routers[0], rid="mictlanx-router-0", host="localhost", port=60666)
    assert_router(routers[1], rid="mictlanx-router-1", host="localhost", port=60667)


def test_parse_legacy_router_format_with_colons():
    # legacy router spec: router_id:host:port
    uri = "mictlanx://r0@127.0.0.1:60666?protocol=http&api_version=4&http2=false"
    routers = MictlanXURI.parse(uri)
    assert len(routers) == 1
    assert_router(routers[0], rid="r0", host="127.0.0.1", port=60666, protocol="http", http2=False, api_version=4)


def test_parse_legacy_ipv6_like_host():
    # legacy format joins all middle parts into host, e.g. IPv6 without brackets
    uri = "mictlanx://r0@2001:db8::1:60666?protocol=http"
    routers = MictlanXURI.parse(uri)
    assert len(routers) == 1
    assert_router(routers[0], rid="r0", host="2001:db8::1", port=60666, protocol="http")


@pytest.mark.parametrize("val,expected", [
    ("1", True), ("0", False),
    ("true", True), ("false", False),
    ("yes", True), ("no", False),
    ("on", True), ("off", False),
    ("weird", False), (None, False),
])
def test_http2_bool_parsing(val, expected, monkeypatch):
    # Compose a URI and test various boolean encodings for http2
    q = f"http2={val}" if val is not None else ""
    sep = "&" if q else ""
    uri = f"mictlanx://r0@h:1?protocol=http&api_version=4{sep}{q}"
    routers = MictlanXURI.parse(uri)
    assert len(routers) == 1
    assert routers[0].http2 == expected


def test_build_canonical_uri_from_objects_and_roundtrip():
    routers_in = [
        AsyncRouter(router_id="r0", ip_addr="localhost", port=60666, protocol="http", http2=False, api_version=4),
        AsyncRouter(router_id="r1", ip_addr="localhost", port=60667, protocol="http", http2=False, api_version=4),
    ]
    # NOTE: build_mictlanx_uri is defined inside the class body without @staticmethod
    # If you haven't decorated it, call it via an instance:
    uri = MictlanXURI().build(routers_in)

    assert uri.startswith("mictlanx://")
    # parse the built URI and compare attributes
    routers_out = MictlanXURI.parse(uri)
    assert len(routers_out) == 2
    for a, b in zip(routers_in, routers_out):
        assert_router(b, rid=a.router_id, host=a.ip_addr, port=a.port,
                      protocol=a.protocol, http2=a.http2, api_version=a.api_version)


def test_whitespace_and_extra_commas_are_ignored():
    uri = "mictlanx://  r0@h:10  , , r1@h:11 ,, ?protocol=http&api_version=4"
    routers = MictlanXURI.parse(uri)
    # print("URI", routers)
    assert [r.router_id for r in routers] == ["r0", "r1"]


# ----------------------
# Error cases
# ----------------------

def test_error_missing_scheme():
    with pytest.raises(ValueError, match="must start with mictlanx://"):
        MictlanXURI.parse("http://r0@h:1?protocol=http")


def test_error_no_routers():
    with pytest.raises(ValueError):
        res = MictlanXURI.parse("mictlanx://?protocol=http")
        print(res)


def test_error_empty_router_spec():
    with pytest.raises(ValueError):
        MictlanXURI.parse("mictlanx://,?protocol=http")


def test_error_invalid_port_non_int():
    with pytest.raises(ValueError):
        res = MictlanXURI.parse("mictlanx://r0@h:abc?protocol=http")
        print("RES", res[0].port)



def test_error_port_out_of_range_low():
    with pytest.raises(ValueError):
        MictlanXURI.parse("mictlanx://r0@h:0?protocol=http")


def test_error_port_out_of_range_high():
    with pytest.raises(ValueError):
        MictlanXURI.parse("mictlanx://r0@h:70000?protocol=http")

def _as_tuple(p: AsyncPeer):
    """Helper to compare AsyncPeer fields easily."""
    return (p.peer_id, p.ip_addr, p.port, p.protocol, p.api_version)


def test_parse_peers_single():
    uri = "mictlanx://peer1@127.0.0.1:5000/?protocol=http&api_version=4"
    peers = MictlanXURI.parse_peers(uri)
    assert len(peers) == 1
    assert _as_tuple(peers[0]) == ("peer1", "127.0.0.1", 5000, "http", 4)


def test_parse_peers_multiple():
    uri = (
        "mictlanx://pA@192.168.0.10:6000,"
        "pB@192.168.0.11:6001"
        "/?protocol=http&api_version=5"
    )
    peers = MictlanXURI.parse_peers(uri)
    tuples = [_as_tuple(p) for p in peers]
    assert tuples == [
        ("pA", "192.168.0.10", 6000, "http", 5),
        ("pB", "192.168.0.11", 6001, "http", 5),
    ]


def test_parse_peers_defaults_when_no_query():
    uri = "mictlanx://nodeX@10.0.0.1:7000"
    peers = MictlanXURI.parse_peers(uri)
    p = peers[0]
    assert p.protocol == "http"   # default
    assert p.api_version == 4     # default


@pytest.mark.parametrize(
    "bad_uri",
    [
        "notmictlanx://id@127.0.0.1:9000",  # wrong scheme
        "mictlanx://",                      # empty
    ],
)
def test_parse_peers_invalid_scheme_or_empty(bad_uri):
    with pytest.raises(ValueError):
        res = MictlanXURI.parse_peers(bad_uri)
        print("RES", res)


@pytest.mark.parametrize(
    "uri",
    [
        "mictlanx://id@host:notaport",  # non-integer port
        "mictlanx://id@host:0",         # port out of range
        "mictlanx://id@host:70000",     # port out of range
    ],
)
def test_parse_peers_invalid_specs(uri):
    with pytest.raises(ValueError):
        MictlanXURI.parse_peers(uri)