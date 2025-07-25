import os
import pytest
from piddiplatsch.cli import cli


def send_message(runner, filename):
    # Ensure the file exists before continuing
    assert os.path.exists(filename), f"Missing file: {filename}"

    # Send the message
    result = runner.invoke(
        cli,
        ["send", filename],
    )

    assert result.exit_code == 0
    assert "📤 Message delivered" in result.output


@pytest.mark.online
def test_send_valid_example(runner, testdata_path):
    filename = os.path.join(
        testdata_path,
        "example.json",
    )

    send_message(runner, filename)


@pytest.mark.online
def test_send_valid_cmip6_mpi_day(runner, testdata_path):
    filename = os.path.join(
        testdata_path,
        "CMIP6",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json",
    )

    send_message(runner, filename)


@pytest.mark.online
def test_send_valid_cmip6_mpi_mon(runner, testdata_path):
    filename = os.path.join(
        testdata_path,
        "CMIP6",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.Amon.tasmin.gn.v20190710.json",
    )

    send_message(runner, filename)


@pytest.mark.online
def test_send_invalid_cmip6_mpi_mon(runner, testdata_path):
    filename = os.path.join(
        testdata_path,
        "CMIP6_invalid",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json",
    )

    send_message(runner, filename)
