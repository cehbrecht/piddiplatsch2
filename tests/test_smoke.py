import pytest
from pathlib import Path
import time
from piddiplatsch.cli import cli

# wait for consumer to process
WAIT_SECS = 1


def assert_dataset_record(handle_client, pid: str):
    record = handle_client.get_record(pid)

    assert record is not None, f"PID {pid} was not registered"
    print(record)
    assert "URL" in record
    assert "AGGREGATION_LEVEL" in record
    assert "DATASET_ID" in record
    # assert "DATASET_VERSION" in record
    assert "HAS_PARTS" in record
    assert "HOSTING_NODE" in record
    assert "REPLICA_NODES" in record
    assert "UNPUBLISHED_REPLICAS" in record
    assert "UNPUBLISHED_HOSTS" in record


def assert_file_record(handle_client, pid: str):
    record = handle_client.get_record(pid)

    assert record is not None, f"PID {pid} was not registered"
    print(record)
    assert "URL" in record
    assert "AGGREGATION_LEVEL" in record


def send_message(runner, filename: Path):
    result = runner.invoke(cli, ["send", filename.as_posix()])

    if result.exit_code != 0 or "📤 Message delivered" not in result.output:
        print("---- CLI Output ----")
        print(result.output)
        print("--------------------")

    assert result.exit_code == 0
    assert "📤 Message delivered" in result.output


@pytest.mark.online
def test_send_valid_example(runner, testfile, handle_client):
    path = testfile("example.json")

    send_message(runner, path)

    time.sleep(WAIT_SECS)

    # TODO: extract the PID dynamically from the file
    pid = "453eed3c-8b9a-31c5-b9c3-a4bb5433cb3d"
    assert_dataset_record(handle_client, pid)


@pytest.mark.online
def test_send_invalid_file(runner):
    result = runner.invoke(cli, ["send", "nonexistent.json"])
    assert result.exit_code != 0
    assert "No such file" in result.output or "Error" in result.output


@pytest.mark.online
def test_send_valid_cmip6_mpi_day(runner, testfile, handle_client):
    path = testfile(
        "CMIP6",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json",
    )

    send_message(runner, path)

    time.sleep(WAIT_SECS)

    assert_dataset_record(handle_client, "bfa39fac-49db-35f1-a5c0-bc67fa7315b0")

    pids = [
        "a5a79818-8ae5-35a7-9cc2-57cffe70d408",
        "20cedc42-2fc5-32c2-9fae-511acfbc8f22",
        "f6b25cf3-844e-32a6-8d07-9b817c90c2ef",
        "d63b6c5e-0595-36f2-8009-e9ad9a0dbd24",
        "8aedb952-e482-3bec-bddd-39b3bca951b3",
    ]
    for pid in pids:
        assert_file_record(handle_client, pid)


@pytest.mark.online
def test_send_valid_cmip6_mpi_mon(runner, testfile, handle_client):
    path = testfile(
        "CMIP6",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.Amon.tasmin.gn.v20190710.json",
    )
    send_message(runner, path)

    time.sleep(WAIT_SECS)

    assert_dataset_record(handle_client, "4f3e6ba6-839d-3e2f-8683-793f8ae66344")

    pids = [
        "a00ed634-4260-3bbd-b7a8-075266d8fd2d",
        "8f72d01f-4bc8-3272-b246-cebe15511d49",
        "5a0ec944-ab03-3900-871c-ccd8ed48f6fd",
        "7980290b-2429-334a-893f-45df2a3ef2e4",
        "aaf8684d-341e-37d2-80bb-854a94a90777",
    ]
    for pid in pids:
        assert_file_record(handle_client, pid)


@pytest.mark.online
def test_send_invalid_cmip6_mpi_mon(runner, testfile):
    path = testfile(
        "CMIP6_invalid",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json",
    )

    send_message(runner, path)
