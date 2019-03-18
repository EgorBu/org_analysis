import argparse
import logging as log

from org_analysis.download_repos import add_download_args, handler as download_handler
from org_analysis.utils import ArgumentDefaultsHelpFormatterNoNone, filter_kwargs


def prepare_parser():
    parser = argparse.ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatterNoNone)
    parser.add_argument("--log-level", default="INFO", choices=log._nameToLevel,
                        help="Logging verbosity.")

    cmds = parser.add_subparsers(help="Commands")

    def add_parser(name, help):
        return cmds.add_parser(
            name, help=help, formatter_class=ArgumentDefaultsHelpFormatterNoNone)

    # download organization/user repositories
    download = add_parser(name="download", help="Download repositories from organization/user.")
    download.set_defaults(handler=download_handler)
    add_download_args(download)

    return parser


def main():
    parser = prepare_parser()
    args = parser.parse_args()
    log.getLogger().setLevel(args.log_level)
    try:
        handler = args.handler
        delattr(args, "handler")
    except AttributeError:
        def print_usage(*args, **kwargs):
            parser.print_usage()

        handler = print_usage
    kwargs = filter_kwargs(vars(args), handler)
    return handler(**kwargs)


if __name__ == "__main__":
    main()
