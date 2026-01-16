from pathlib import Path

import pytest

from piddiplatsch.testing.kafka_client import send_message_to_kafka

pytestmark = pytest.mark.smoke


def assert_dataset_record(handle_client, pid: str, all: bool = False):
    record = handle_client.get(pid)
    assert record is not None, f"PID {pid} was not registered"
    print(record)
    assert "URL" in record
    assert "AGGREGATION_LEVEL" in record
    assert "DATASET_ID" in record
    assert "DATASET_VERSION" in record
    assert "HOSTING_NODE" in record
    if all:
        assert "HAS_PARTS" in record


def assert_file_record(handle_client, pid: str):
    record = handle_client.get(pid)
    assert record is not None, f"PID {pid} was not registered"
    print(record)
    assert "URL" in record
    assert "AGGREGATION_LEVEL" in record
    assert "FILE_NAME" in record
    assert "IS_PART_OF" in record
    assert "DOWNLOAD_URL" in record
    assert "CHECKSUM" in record
    assert "CHECKSUM_METHOD" in record
    assert "FILE_SIZE" in record


def assert_record(handle_client, pid, sub_pids, all: bool = False):
    wait_for_pid(handle_client, pid)
    assert_dataset_record(handle_client, pid, all)
    for sub_pid in sub_pids:
        assert_file_record(handle_client, sub_pid)


def wait_for_pid(handle_client, pid: str, timeout: float = 15.0):
    """Wait until a PID is available in the handle service or timeout."""
    import time

    start = time.time()
    while time.time() - start < timeout:
        if handle_client.get(pid):
            return
        time.sleep(0.2)
    raise AssertionError(f"PID {pid} was not registered within {timeout:.1f} seconds")


# ----------------------------
# Smoke Tests
# ----------------------------


def test_send_valid_cmip6_mri_6hr_dc4(testfile, handle_client):
    p: Path = testfile(
        "data_challenge_04",
        "CMIP6",
        "CMIP6.HighResMIP.MRI.MRI-AGCM3-2-H.highresSST-present.r1i1p1f1.6hrPlevPt.psl.gn.v20190820.json",
    )
    send_message_to_kafka(p)

    pid = "b06058a6-1077-35cb-9500-1ccbd341d309"
    pids = [
        "7afa385c-faa4-3a5f-85e6-79da95ef3add",
        "d58dc83b-a23a-308f-aada-1b2b7779d99a",
        "19d85ef9-bf10-3712-9ad5-c2e9cf124c7e",
        "321fff12-488d-3605-8e56-c1c4e75f4561",
        "1e032121-8899-3fd0-9d81-380083338a29",
        "0b469be3-c851-3c37-96fd-f0f30dd90809",
        "4764e73c-ee61-38d7-aed5-4e35b8c1b39b",
    ]
    assert_record(handle_client, pid, pids)


@pytest.mark.skip(reason="checksum failure")
def test_send_valid_cmip6_ipsl_mon_dc4(testfile, handle_client):
    p: Path = testfile(
        "data_challenge_04",
        "CMIP6",
        "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp245.r1i1p1f1.Amon.pr.gr.v20190119.json",
    )
    send_message_to_kafka(p)

    pid = "11da5bd1-157f-3158-b775-ba42ed4e193b"
    pids = ["d1e2181e-1066-3d33-b56a-f45bf7a40ab5"]
    assert_record(handle_client, pid, pids)


def test_send_invalid_cmip6_dkrz_yr_dc4(testfile):
    p: Path = testfile(
        "data_challenge_04",
        "CMIP6_invalid",
        "CMIP6.ScenarioMIP.DKRZ.MPI-ESM1-2-HR.ssp126.r1i1p1f1.Eyr.baresoilFrac.gn.v20190710.json",
    )
    send_message_to_kafka(p)


def test_send_invalid_cmip6_ipsl_mon_dc4_missing_file_size(testfile):
    p: Path = testfile(
        "data_challenge_04",
        "CMIP6_invalid",
        "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp245.r1i1p1f1.Amon.pr.gr.v20190119_missing_file_size.json",
    )
    send_message_to_kafka(p)


def test_send_multiple_files(testfile, handle_client):
    files = [
        testfile(
            "data_challenge_04",
            "CMIP6",
            "CMIP6.HighResMIP.MRI.MRI-AGCM3-2-H.highresSST-present.r1i1p1f1.6hrPlevPt.psl.gn.v20190820.json",
        ),
        testfile(
            "data_challenge_04",
            "CMIP6",
            "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp245.r1i1p1f1.Amon.pr.gr.v20190119.json",
        ),
    ]
    for p in files:
        send_message_to_kafka(p)
