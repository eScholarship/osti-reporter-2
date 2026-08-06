def process_args():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("-iq", "--input-qa",
                        dest="input_qa",
                        action="store_true",
                        default=False,
                        help="Use Elements and eSchol QA for input.")

    parser.add_argument("-eq", "--elink-qa",
                        dest="elink_qa",
                        action="store_true",
                        default=False,
                        help="Submit new OSTI pubs to eLink's QA servers.")

    parser.add_argument("-oq", "--output-qa",
                        dest="output_qa",
                        action="store_true",
                        default=False,
                        help="Sends updates (submissions w/ OSTI IDs) to QA eschol_osti_db")

    parser.add_argument("-mu", "--metadata-updates",
                        dest="metadata_updates",
                        action="store_true",
                        default=False,
                        help="Optional. If this flag is included, the program will send metadata updates \
                            to OSTI for publications already in their database. Default is FALSE.")

    parser.add_argument("-pu", "--pdf-updates",
                        dest="pdf_updates",
                        action="store_true",
                        default=False,
                        help="Optional. If this flag is included, the program will send updated PDFs \
                            to OSTI for publications already in their database. Default is FALSE.")

    parser.add_argument("-iu", "--individual-updates",
                        dest="individual_updates",
                        type=int,
                        default=[],
                        nargs="+",
                        help="Optional. Use this flag to specify individual publication IDs for \
                            updates. Example: -iu 1234567 0129384 6592834")

    parser.add_argument("-x", "--test",
                        dest="test",
                        action="store_true",
                        default=False,
                        help="Outputs update XML or JSON to disk rather than sending to OSTI API.")

    parser.add_argument("-uo", "--updates-only",
                        dest="updates_only",
                        action="store_true",
                        default=False,
                        help=("Skips ordinary submission step only runs updates. "
                              "Will exit if not also run with -mu (media updates) or -pu (pdf updates)."))

    parser.add_argument("-fl", "--full-logging",
                        dest="full_logging",
                        action="store_true",
                        default=False,
                        help="Outputs: Temp table sql query and results; Submission and response files.")

    parser.add_argument("-oco", "--output-concurrence-override",
                        dest="output_override",
                        action="store_true",
                        default=False,
                        help="A Safeguard -- must be added if sending -eq and -oq to different connections.")

    args = parser.parse_args()

    if (args.output_qa != args.elink_qa) and not args.output_override:
        raise RuntimeError("SAFETY CHECK!!! --elink-qa and --output-qa do not match. "
                           "Run with -oco if this is actually intended. Exiting.")

    return args


def assign_creds(args):

    from pub_oapi_tools_common import aws_lambda

    # Arg switches
    input_env = "qa" if args.input_qa else "prod"
    elink_env = "qa" if args.elink_qa else "prod"
    output_env = "qa" if args.output_qa else "prod"

    selected_creds = {}

    param_req = {
        # Elements DB for input
        'elements_reporting_db': {
            'folder': 'pub-oapi-tools/elements-reporting-db',
            'env': input_env
        },
        # CDL MySQL for input (read)
        'cdl_db_read': {
            'folder': 'pub-oapi-tools/tools-rds',
            'env': input_env,
            'names': ['user', 'password', 'server', 'port', 'osti-db', 'driver', 'osti-table']
        },
        # CDL MySQL for output (write)
        'cdl_db_write': {
            'folder': 'pub-oapi-tools/tools-rds',
            'env': output_env,
            'names': ['user', 'password', 'server', 'port', 'osti-db', 'driver', 'osti-table']
        },
        # OSTI Elink
        'osti_api': {
            'folder': 'pub-oapi-tools/elink-api',
            'env': elink_env,
            'names': ['endpoint', 'token', 'pdf-user-agent']
        },
        # eSchol API
        'eschol_api': {
            'folder': 'pub-oapi-tools/eschol-api',
            'env': output_env,
            'names': ['endpoint', 'priv-key', 'cookie']
        }
    }

    params = aws_lambda.get_parameters(param_req=param_req)
    return params
