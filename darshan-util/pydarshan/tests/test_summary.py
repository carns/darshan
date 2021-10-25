import os
import pytest
import argparse
from unittest import mock
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal #type: ignore

import darshan
from darshan.cli import summary


@pytest.mark.parametrize(
    "argv", [
        ["./tests/input/sample.darshan"],
        ["./tests/input/sample.darshan", "--output=test.html"],
    ]
)
def test_setup_parser(argv):
    # test for summary.setup_parser() to verify the
    # correct arguments are being added to the parser

    with mock.patch("sys.argv", argv):
        # initialize the parser
        parser = argparse.ArgumentParser(description="")
        # run through setup_parser()
        summary.setup_parser(parser=parser)
        # parse the input arguments
        args = parser.parse_args(argv)

    # verify the description has been added
    assert parser.description == "Generates a Darshan Summary Report"
    # verify the log path and output filenames are correct
    assert args.log_path == "./tests/input/sample.darshan"
    if args.output:
        assert args.output == "test.html"


@pytest.mark.parametrize(
    "argv", [
        [os.path.abspath("./examples/example-logs/dxt.darshan")],
        [os.path.abspath("./examples/example-logs/dxt.darshan"), "--output=test.html"],
    ]
)
def test_main_with_args(tmpdir, argv):
    # test summary.main() by building a parser
    # and using it as an input

    # initialize the parser, add the arguments, and parse them
    with mock.patch("sys.argv", argv):
        parser = argparse.ArgumentParser(description="")
        summary.setup_parser(parser=parser)
        args = parser.parse_args(argv)

    with tmpdir.as_cwd():
        # run main() using the arguments built by the parser
        summary.main(args=args)

        # get the expected save path
        if len(argv) == 1:
            output_fname = "dxt_report.html"
        else:
            output_fname = "test.html"
        expected_save_path = os.path.abspath(output_fname)

        # verify the HTML file was generated
        assert os.path.exists(expected_save_path)


@pytest.mark.parametrize(
    "argv", [
        [os.path.abspath("./tests/input/noposix.darshan")],
        [os.path.abspath("./tests/input/noposix.darshan"), "--output=test.html"],
        [os.path.abspath("./tests/input/sample-dxt-simple.darshan")],
        [os.path.abspath("./tests/input/sample-dxt-simple.darshan"), "--output=test.html"],
        [None],
    ]
)
def test_main_without_args(tmpdir, argv):
    # test summary.main() by running it without a parser

    with mock.patch("sys.argv", [""] + argv):
        if argv[0]:
            # if a log path is given, generate the summary report
            # in a temporary directory
            with tmpdir.as_cwd():
                summary.main()

                # get the path for the generated summary report
                if len(argv) == 1:
                    log_fname = os.path.basename(argv[0])
                    output_fname = os.path.splitext(log_fname)[0] + "_report.html"
                else:
                    output_fname = "test.html"
                expected_save_path = os.path.abspath(output_fname)

                # verify the HTML file was generated
                assert os.path.exists(expected_save_path)

        else:
            # if no log path is given expect a runtime error
            # due to a failure to open the file
            with pytest.raises(RuntimeError):
                summary.main()


@pytest.mark.skipif(not pytest.has_log_repo, # type: ignore
                    reason="missing darshan_logs")
def test_main_all_logs_repo_files(tmpdir, log_repo_files):
    # similar to `test_main_without_args` but focused
    # on the Darshan logs from the logs repo:
    # https://github.com/darshan-hpc/darshan-logs

    for log_filepath in log_repo_files:
        argv = [log_filepath]
        with mock.patch("sys.argv", [""] + argv):
            with tmpdir.as_cwd():
                # generate the summary report
                summary.main()

                # get the path for the generated summary report
                log_fname = os.path.basename(argv[0])
                output_fname = os.path.splitext(log_fname)[0] + "_report.html"
                expected_save_path = os.path.abspath(output_fname)

                # verify the HTML file was generated
                assert os.path.exists(expected_save_path)


class TestReportData:

    @pytest.mark.parametrize(
        "log_path",
        [
            "tests/input/sample.darshan",
            "tests/input/noposix.darshan",
            "tests/input/sample-badost.darshan",
            "tests/input/sample-dxt-simple.darshan",
        ],
    )
    def test_stylesheet(self, log_path):
        # check that the report object is
        # generating the correct attributes
        R = summary.ReportData(log_path=log_path)
        # verify the first line shows up correctly for each log
        expected_str = "p {\n  font-size: 12px;\n}"
        assert expected_str in R.stylesheet

    @pytest.mark.parametrize(
        "log_path, expected_header",
        [
            ("tests/input/sample.darshan", "vpicio_uni (2017-03-20)"),
            # anonymized case
            ("tests/input/noposix.darshan", "Anonymized (2018-01-02)"),
            ("tests/input/sample-badost.darshan", "ior (2017-06-20)"),
            ("tests/input/sample-dxt-simple.darshan", "a.out (2021-04-22)"),
            ("examples/example-logs/dxt.darshan", "N/A (2020-04-21)"),
        ],
    )
    def test_header_and_footer(self, log_path, expected_header):
        # check the header and footer strings stored in the report data object
        R = summary.ReportData(log_path=log_path)
        assert R.header == expected_header
        assert "Summary report generated via PyDarshan v" in R.footer

    @pytest.mark.parametrize(
        "log_path, expected_df",
        [
            (
                "tests/input/sample.darshan",
                pd.DataFrame(
                    index=[
                        "Job ID", "User ID", "# Processes", "Runtime (s)",
                        "Start Time", "End Time", "Command", "Log Filename",
                        "Runtime Library Version", "Log Format Version"
                    ],
                    data=[
                        "4478544",
                        "69615",
                        "2048",
                        "116.0",
                        str(datetime.fromtimestamp(1490000867)),
                        str(datetime.fromtimestamp(1490000983)),
                        (
                            "/global/project/projectdirs/m888/glock/tokio-abc-"
                            "results/bin.edison/vpicio_uni /scratch2/scratchdirs"
                            "/glock/tokioabc-s.4478544/vpicio/vpicio.hdf5 32"
                        ),
                        "sample.darshan",
                        "3.1.3",
                        "3.10",
                    ]
                )
            ),
            # anonymized case
            (
                "tests/input/noposix.darshan",
                pd.DataFrame(
                    index=[
                        "Job ID", "User ID", "# Processes", "Runtime (s)",
                        "Start Time", "End Time", "Command", "Log Filename",
                        "Runtime Library Version", "Log Format Version"
                    ],
                    data=[
                        "83017637",
                        "996599276",
                        "512",
                        "39212.0",
                        str(datetime.fromtimestamp(1514923055)),
                        str(datetime.fromtimestamp(1514962267)),
                        "Anonymized",
                        "noposix.darshan",
                        "3.1.4",
                        "3.10",
                    ]
                )
            ),
            (
                "tests/input/sample-dxt-simple.darshan",
                pd.DataFrame(
                    index=[
                        "Job ID", "User ID", "# Processes", "Runtime (s)",
                        "Start Time", "End Time", "Command", "Log Filename",
                        "Runtime Library Version", "Log Format Version"
                    ],
                    data=[
                    "4233209",
                    "28751",
                    "16",
                    "< 1",
                    str(datetime.fromtimestamp(1619109091)),
                    str(datetime.fromtimestamp(1619109091)),
                    (
                        "/yellow/usr/projects/eap/users/treddy"
                        "/simple_dxt_mpi_io_darshan/a.out"
                    ),
                    "sample-dxt-simple.darshan",
                    "3.2.1",
                    "3.21",
                    ]
                )
            ),
        ],
    )
    def test_metadata_table(self, log_path, expected_df):
        # regression test for `summary.ReportData.get_metadata_table()`

        # generate the report data
        R = summary.ReportData(log_path=log_path)
        # convert the metadata table back to a pandas dataframe
        actual_metadata_df = pd.read_html(R.metadata_table, index_col=0)[0]
        # correct index and columns attributes after
        # `index_col` removed the first column
        actual_metadata_df.index.names = [None]
        actual_metadata_df.columns = [0]

        # check the metadata dataframes
        assert_frame_equal(actual_metadata_df, expected_df)


    @pytest.mark.parametrize(
        "log_path, expected_df",
        [
            # each of these logs offers a unique
            # set of modules to verify
            (
                "tests/input/sample.darshan",
                pd.DataFrame(
                    index=[
                        "POSIX (ver=3)", "MPI-IO (ver=2)",
                        "LUSTRE (ver=1)", "STDIO (ver=1)",
                    ],
                    data=[["0.18 KiB"], ["0.15 KiB"], ["0.08 KiB"], ["3.16 KiB"]],
                ),
            ),
            (
                "tests/input/noposix.darshan",
                pd.DataFrame(
                    index=["LUSTRE (ver=1)", "STDIO (ver=1)"],
                    data=[["6.07 KiB"], ["0.21 KiB"]],
                )
            ),
            (
                "tests/input/noposixopens.darshan",
                pd.DataFrame(
                    index=["POSIX (ver=3)", "STDIO (ver=1)"],
                    data=[["0.04 KiB"], ["0.27 KiB"]],
                )
            ),
            (
                "tests/input/sample-goodost.darshan",
                pd.DataFrame(
                    index=["POSIX (ver=3)", "LUSTRE (ver=1)", "STDIO (ver=1)"],
                    data=[["5.59 KiB"], ["1.47 KiB"], ["0.07 KiB"]],
                )
            ),
            (
                "tests/input/sample-dxt-simple.darshan",
                pd.DataFrame(
                    index=[
                        "POSIX (ver=4)", "MPI-IO (ver=3)",
                        "DXT_POSIX (ver=1)", "DXT_MPIIO (ver=2)",
                    ],
                    data=[["2.94 KiB"], ["1.02 KiB"], ["0.08 KiB"], ["0.06 KiB"]],
                )
            )
        ],
    )
    def test_module_table(self, log_path, expected_df):
        # regression test for `summary.ReportData.get_module_table()`

        # collect the report data
        R = summary.ReportData(log_path=log_path)
        # convert the module table back to a pandas dataframe
        actual_mod_df = pd.read_html(R.module_table, index_col=0)[0]
        # correct index and columns attributes after
        # `index_col` removed the first column
        actual_mod_df.index.names = [None]
        actual_mod_df.columns = [0]

        # verify the number of modules in the report is equal to
        # the number of rows in the module table
        expected_module_count = len(R.report.modules.keys())
        assert actual_mod_df.shape[0] == expected_module_count

        # check the module dataframes
        assert_frame_equal(actual_mod_df, expected_df)

    @pytest.mark.parametrize(
        "report, expected_cmd",
        [
            (
                darshan.DarshanReport("tests/input/sample.darshan"),
                (
                    "/global/project/projectdirs/m888/glock/tokio-abc-results/"
                    "bin.edison/vpicio_uni /scratch2/scratchdirs/glock/tokioabc"
                    "-s.4478544/vpicio/vpicio.hdf5 32"
                ),
            ),
            (
                darshan.DarshanReport("tests/input/sample-badost.darshan"),
                (
                    "/global/project/projectdirs/m888/glock/tokio-abc-results/"
                    "bin.edison/ior -H -k -w -o ior-posix.out -s 64 -f /global"
                    "/project/projectdirs/m888/glock/tokio-abc-results/inputs/"
                    "posix1m2.in"
                ),
            ),
            (
                darshan.DarshanReport("tests/input/sample-goodost.darshan"),
                (
                    "/global/homes/g/glock/src/git/ior-lanl/src/ior "
                    "-t 8m -b 256m -s 4 -F -C -e -a POSIX -w -k"
                ),
            ),
            (
                darshan.DarshanReport("tests/input/sample-dxt-simple.darshan"),
                "/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/a.out ",
            ),
            # anonymized cases
            (darshan.DarshanReport("tests/input/noposix.darshan"), "Anonymized"),
            (darshan.DarshanReport("tests/input/noposixopens.darshan"), "Anonymized"),
            # no executable case
            (darshan.DarshanReport("examples/example-logs/dxt.darshan"), "N/A"),
        ],
    )
    def test_get_full_command(self, report, expected_cmd):
        # regression test for `summary.ReportData.get_full_command()`
        actual_cmd = summary.ReportData.get_full_command(report=report)
        assert actual_cmd == expected_cmd

    @pytest.mark.parametrize(
        "report, expected_runtime",
        [
            (darshan.DarshanReport("tests/input/sample.darshan"), "116.0",),
            (darshan.DarshanReport("tests/input/noposix.darshan"), "39212.0"),
            (darshan.DarshanReport("tests/input/noposixopens.darshan"), "1110.0"),
            (darshan.DarshanReport("tests/input/sample-badost.darshan"), "779.0",),
            (darshan.DarshanReport("tests/input/sample-goodost.darshan"), "4.0",),
            # special case where the calculated run time is 0
            (darshan.DarshanReport("tests/input/sample-dxt-simple.darshan"), "< 1",),
        ],
    )
    def test_get_runtime(self, report, expected_runtime):
        # regression test for `summary.ReportData.get_runtime()`
        actual_runtime = summary.ReportData.get_runtime(report=report)
        assert actual_runtime == expected_runtime
