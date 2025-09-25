from piddiplatsch.records import CMIP6FileRecord
from piddiplatsch.records.cmip6_file_record import extract_asset_records
from piddiplatsch.utils.models import parse_pid

# Minimal mock STAC item
STAC_ITEM = {
    "id": "test-item",
    "assets": {
        "data.nc": {
            "href": "https://example.com/data.nc",
            "tracking_id": None,
            "file:checksum": "9123456789abcdef0123456789abcdef01234568",
            "file:size": "1024",
            "alternate": {"mirror": {"href": "https://mirror.com/data.nc"}},
        },
        "readme.txt": {
            "href": "https://example.com/readme.txt",
            "tracking_id": "hdl:21.TEST/readme123",
        },
    },
}


def test_asset_property():
    record = CMIP6FileRecord(STAC_ITEM, "data.nc", strict=False)
    assert record.asset["href"] == "https://example.com/data.nc"
    assert record.alternates == {"mirror": {"href": "https://mirror.com/data.nc"}}


def test_get_value_with_alternates():
    record = CMIP6FileRecord(STAC_ITEM, "data.nc", strict=False)
    # Existing key
    assert record.get_value("href") == "https://example.com/data.nc"
    # Key only in alternate
    assert record.get_value("href_nonexistent") == ""  # key missing in both


def test_tracking_id_and_pid():
    # asset without tracking_id generates PID
    record = CMIP6FileRecord(STAC_ITEM, "data.nc", strict=False)
    pid = record.pid
    assert pid is not None

    # asset with existing tracking_id uses it
    record2 = CMIP6FileRecord(STAC_ITEM, "readme.txt", strict=False)
    assert parse_pid(record2.tracking_id) == "readme123"
    assert record2.pid == "readme123"


def test_parent_and_filename():
    record = CMIP6FileRecord(STAC_ITEM, "data.nc", strict=False)
    assert record.parent.startswith("hdl:")  # parent PID URI
    assert record.filename == "data.nc"


def test_download_and_replica_urls():
    record = CMIP6FileRecord(STAC_ITEM, "data.nc", strict=False)
    assert record.download_url == "https://example.com/data.nc"
    assert record.replica_download_urls == ["https://mirror.com/data.nc"]


def test_checksum_and_size():
    record = CMIP6FileRecord(STAC_ITEM, "data.nc", strict=False)
    assert record.checksum == "9123456789abcdef0123456789abcdef01234568"
    assert record.size == 1024


def test_as_handle_model_structure():
    record = CMIP6FileRecord(STAC_ITEM, "data.nc", strict=False)
    model = record.as_handle_model()
    assert str(model.URL) == record.url
    assert model.FILE_NAME == "data.nc"
    assert model.CHECKSUM == "9123456789abcdef0123456789abcdef01234568"
    assert model.FILE_SIZE == 1024
    assert str(model.DOWNLOAD_URL) == record.download_url
    assert [
        str(url) for url in model.REPLICA_DOWNLOAD_URLS
    ] == record.replica_download_urls
    assert model._PID == record.pid


def test_extract_asset_records_excludes():
    records = extract_asset_records(
        STAC_ITEM, exclude_keys=["readme.txt"], strict=False
    )
    assert all(r.asset_key != "readme.txt" for r in records)
    assert any(r.asset_key == "data.nc" for r in records)
