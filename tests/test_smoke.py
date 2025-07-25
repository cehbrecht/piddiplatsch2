import pytest
from pathlib import Path
from piddiplatsch.cli import cli


def send_message(runner, filename: Path):
    result = runner.invoke(
        cli,
        ["send", filename.as_posix()],
    )

    assert result.exit_code == 0
    assert "📤 Message delivered" in result.output


@pytest.mark.online
def test_send_valid_example(runner, testfile):
    path = testfile("example.json")

    send_message(runner, path)


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
