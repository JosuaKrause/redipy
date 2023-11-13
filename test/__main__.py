"""Offers functionality for organizing test timings. Tests get assigned to
continuous integration nodes based on their timings. The goal is to have a
roughly equal test runtime on all nodes."""
import argparse

from .mng import merge_results, split_tests


def parse_args_split_tests(parser: argparse.ArgumentParser) -> None:
    """
    Add splitting tests arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser.
    """
    parser.add_argument(
        "--filepath",
        default="test-results/results.xml",
        type=str,
        help="xml with previous test timings")
    parser.add_argument(
        "--total-nodes",
        type=int,
        help="number of test runner nodes")
    parser.add_argument(
        "--node-id",
        type=int,
        help="id of the current test runner node")


def parse_args_merge_results(parser: argparse.ArgumentParser) -> None:
    """
    Add merging tests arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser.
    """
    parser.add_argument(
        "--dir",
        default="test-results",
        type=str,
        help=(
            "base test result folder. needs subfolder 'parts' "
            "containing all the xml files to join."))
    parser.add_argument(
        "--out-fname",
        type=str,
        help="output file of combined xml")


def parse_args() -> argparse.Namespace:
    """
    Parses the command line arguments.

    Returns:
        argparse.Namespace: The parse result.
    """
    parser = argparse.ArgumentParser(
        description="Test Utilities")
    subparser = parser.add_subparsers(title="Commands")

    def run_split_tests(args: argparse.Namespace) -> None:
        split_tests(
            filepath=args.filepath,
            total_nodes=args.total_nodes,
            cur_node=args.node_id)

    subparses_split_tests = subparser.add_parser("split")
    subparses_split_tests.set_defaults(func=run_split_tests)
    parse_args_split_tests(subparses_split_tests)

    def run_merge_results(args: argparse.Namespace) -> None:
        merge_results(
            base_folder=args.dir,
            out_filename=args.out_fname)

    subparses_merge_results = subparser.add_parser("merge")
    subparses_merge_results.set_defaults(func=run_merge_results)
    parse_args_merge_results(subparses_merge_results)
    return parser.parse_args()


def run() -> None:
    """
    Runs the functionality.
    """
    args = parse_args()
    args.func(args)


if __name__ == "__main__":
    run()
