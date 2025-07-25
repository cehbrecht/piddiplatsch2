import os
import pytest
from piddiplatsch.cli import cli


def send_and_consume_message(runner, filename):
    # Send the message
    result_send = runner.invoke(
        cli,
        ["send", filename],
    )

    assert result_send.exit_code == 0
    assert "📤 Message delivered" in result_send.output


@pytest.mark.online
def test_send_valid_example(runner, testdata_path):
    filename = os.path.join(
        testdata_path,
        "example.json",
    )

    send_and_consume_message(runner, filename)


@pytest.mark.online
def test_send_valid_cmip6_mpi(runner, testdata_path):
    # Path to the JSON file in tests/testdata
    json_path = os.path.join(
        testdata_path,
        "CMIP6",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json",
    )

    # Ensure the file exists before continuing
    assert os.path.exists(json_path), f"Missing file: {json_path}"

    send_and_consume_message(runner, json_path)
