def process_args():
    import argparse

    parser = argparse.ArgumentParser()

    def validate_connection(arg):
        arg = arg.lower()
        if not (arg == "qa" or arg == "production"):
            raise ValueError
        return arg

    def validate_elink_version(arg):
        if not (arg == '1' or arg == '2'):
            raise ValueError
        return int(arg)

    parser.add_argument("-c", "--connection",
                        dest="connection",
                        type=validate_connection,
                        help="REQUIRED. Specify ONLY 'qa' or 'production'")

    parser.add_argument("-t", "--tunnel",
                        dest="tunnel_needed",
                        action="store_true",
                        default=False,
                        help="Optional. Include to run the connection through a tunnel.")

    parser.add_argument("-u", "--updates",
                        dest="send_updates",
                        action="store_true",
                        default=False,
                        help="Optional. If this flag is included, the program will send updates to OSTI \
                            for publications already in their database. Default is false, where only \
                            new publications are sent.")

    parser.add_argument("-v", "--version",
                        dest="elink_version",
                        type=validate_elink_version,
                        default=2,
                        help="Specify OSTI elink version 1 or 2 (default)")

    parser.add_argument("-x", "--test",
                        dest="test",
                        action="store_true",
                        default=False,
                        help="Outputs update XML or JSON to disk rather than sending to OSTI API.")

    return parser.parse_args()
