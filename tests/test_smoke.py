import pytest
from pathlib import Path
import time
import json
from piddiplatsch.cli import cli


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

    time.sleep(1)  # wait for consumer to process

    # TODO: extract the PID dynamically from the file
    pid = "453eed3c-8b9a-31c5-b9c3-a4bb5433cb3d"
    record = handle_client.get_item(pid)

    assert record is not None, f"PID {pid} was not registered"
    print(record)
    assert "URL" in record, f"PID {pid} missing 'URL' field"


@pytest.mark.online
def test_send_invalid_file(runner):
    result = runner.invoke(cli, ["send", "nonexistent.json"])
    assert result.exit_code != 0
    assert "No such file" in result.output or "Error" in result.output


@pytest.mark.online
def test_send_valid_cmip6_mpi_day(runner, testfile):
    path = testfile(
        "CMIP6",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json",
    )

    send_message(runner, path)


@pytest.mark.online
def test_send_valid_cmip6_mpi_mon(runner, testfile):
    path = testfile(
        "CMIP6",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.Amon.tasmin.gn.v20190710.json",
    )
    send_message(runner, path)


@pytest.mark.online
def test_send_invalid_cmip6_mpi_mon(runner, testfile):
    path = testfile(
        "CMIP6_invalid",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json",
    )

    send_message(runner, path)
