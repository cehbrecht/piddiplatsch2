import string
from datetime import datetime
from uuid import NAMESPACE_URL, UUID, uuid3

import pytest
from hypothesis import given
from hypothesis import strategies as st

import piddiplatsch.utils.models as models

# ----------------------
# build_handle tests
# ----------------------


def test_build_handle_with_and_without_uri():
    pid = "12345"
    assert models.build_handle(pid) == "21.TEST/12345"
    assert models.build_handle(pid, as_uri=True) == "hdl:21.TEST/12345"


# ----------------------
# item_pid and asset_pid tests
# ----------------------


def test_item_pid_is_deterministic():
    item_id = "item-001"
    pid1 = models.item_pid(item_id)
    pid2 = models.item_pid(item_id)

    assert pid1 == pid2
    assert pid1 == str(uuid3(NAMESPACE_URL, item_id))


@given(st.text(alphabet=string.printable))
def test_item_pid_always_valid_uuid(item_id):
    pid = models.item_pid(item_id)
    assert isinstance(UUID(pid), UUID)


def test_asset_pid_is_deterministic():
    item_id = "item-001"
    asset_key = "asset-a"

    pid1 = models.asset_pid(item_id, asset_key)
    pid2 = models.asset_pid(item_id, asset_key)

    assert pid1 == pid2
    assert pid1 == str(uuid3(NAMESPACE_URL, f"{item_id}#{asset_key}"))


@given(
    item_id=st.text(alphabet=string.printable),
    asset_key=st.text(alphabet=string.printable),
)
def test_asset_pid_always_valid_uuid(item_id, asset_key):
    pid = models.asset_pid(item_id, asset_key)
    assert isinstance(UUID(pid), UUID)


# ----------------------
# drop_empty tests
# ----------------------


@pytest.mark.parametrize(
    "data,expected",
    [
        ({"a": "", "b": None, "c": [], "d": {}, "e": 42}, {"e": 42}),
        ({"nested": {"x": "", "y": "val"}}, {"nested": {"y": "val"}}),
        ({}, {}),
    ],
)
def test_drop_empty(data, expected):
    assert models.drop_empty(data) == expected


@given(
    st.dictionaries(
        st.text(alphabet=string.printable, min_size=1, max_size=5),
        st.one_of(
            st.none(),
            st.integers(),
            st.text(alphabet=string.printable, min_size=1, max_size=10),
        ),
        max_size=5,
    )
)
def test_drop_empty_no_empty_fields_in_result(d):
    cleaned = models.drop_empty(d)
    assert all(v not in ("", [], {}, None) for v in cleaned.values())


# ----------------------
# parse_datetime tests
# ----------------------


def test_parse_datetime_valid_and_invalid():
    valid = "2021-09-01T12:34:56Z"
    dt = models.parse_datetime(valid)
    assert isinstance(dt, datetime)
    assert dt.isoformat() == "2021-09-01T12:34:56+00:00"

    invalid = "not-a-datetime"
    result = models.parse_datetime(invalid)
    assert result is None

    assert models.parse_datetime(None) is None


@given(st.datetimes())
def test_parse_datetime_roundtrip(dt):
    iso = dt.isoformat()
    parsed = models.parse_datetime(iso)
    assert isinstance(parsed, datetime)


# ----------------------
# parse_pid tests
# ----------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("21.TEST/abc123", "abc123"),
        ("abc123", "abc123"),
        ("", ""),
        (None, None),
    ],
)
def test_parse_pid(value, expected):
    assert models.parse_pid(value) == expected


@given(st.text(alphabet=string.printable))
def test_parse_pid_never_crashes(s):
    result = models.parse_pid(s)
    assert result is None or isinstance(result, str)


# ----------------------
# detect_checksum_type tests
# ----------------------


@pytest.mark.parametrize(
    "checksum,expected",
    [
        ("d41d8cd98f00b204e9800998ecf8427e", "MD5"),
        ("a" * 40, "SHA1"),
        ("a" * 64, "SHA256"),
        ("a" * 128, "SHA512"),
        ("1220" + "a" * 64, "SHA256-MULTIHASH"),
        ("zzz", "UNKNOWN"),
    ],
)
def test_detect_checksum_type(checksum, expected):
    assert models.detect_checksum_type(checksum) == expected


@given(st.text(alphabet="0123456789abcdef", min_size=1, max_size=200))
def test_detect_checksum_type_known_lengths(hexstr):
    length = len(hexstr)
    algo = models.detect_checksum_type(hexstr)

    if hexstr.startswith("1220") and length == 68:
        assert algo == "SHA256-MULTIHASH"
    elif length == 32:
        assert algo == "MD5"
    elif length == 40:
        assert algo == "SHA1"
    elif length == 64:
        assert algo == "SHA256"
    elif length == 128:
        assert algo == "SHA512"
    else:
        assert algo in {"UNKNOWN", "SHA256-MULTIHASH"}
