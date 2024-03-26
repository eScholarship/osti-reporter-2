def process_args():
    import argparse

    parser = argparse.ArgumentParser()

    def validate_elink_version(arg):
        if not (arg == '1' or arg == '2'):
            raise ValueError
        return int(arg)

    parser.add_argument("-iq", "--input-qa",
                        dest="input_qa",
                        action="store_true",
                        default=False,
                        help="Use Elements and eSchol QA for input.")

    parser.add_argument("-eq", "--elink-qa",
                        dest="elink_qa",
                        action="store_true",
                        default=False,
                        help="Submit to eLink's QA servers (works w/ eLink v1 and v2).")

    parser.add_argument("-oq", "--output-qa",
                        dest="output_qa",
                        action="store_true",
                        default=False,
                        help="Sends updates (submissions w/ OSTI IDs) to QA eschol_osti_db")

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
                            for publications already in their database. Default is FALSE, e.g. only \
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


def assign_creds(args):
    import creds
    selected_creds = {}

    # SSH tunnel
    if args.input_qa:
        selected_creds['ssh'] = creds.ssh_creds_qa
    else:
        selected_creds['ssh'] = creds.ssh_creds_prod

    # Elements reporting db MSSQL
    if args.input_qa:
        if args.tunnel_needed:
            selected_creds['elements_reporting_db'] = creds.elements_reporting_db_local_qa
        else:
            selected_creds['elements_reporting_db'] = creds.elements_reporting_db_server_qa
    else:
        if args.tunnel_needed:
            selected_creds['elements_reporting_db'] = creds.elements_reporting_db_local_prod
        else:
            selected_creds['elements_reporting_db'] = creds.elements_reporting_db_server_prod

    # eSchol MySQL for input (read)
    if args.input_qa:
        selected_creds['eschol_db_read'] = creds.eschol_osti_db_qa
    else:
        selected_creds['eschol_db_read'] = creds.eschol_osti_db_prod

    # eSchol MySQL for output (write)
    if args.output_qa:
        selected_creds['eschol_db_write'] = creds.eschol_osti_db_qa
    else:
        selected_creds['eschol_db_write'] = creds.eschol_osti_db_prod

    # OSTI Elink
    if args.elink_version == 1:
        if args.elink_qa:
            selected_creds['osti_api'] = creds.osti_v1_qa
        else:
            selected_creds['osti_api'] = creds.osti_v1_prod
    else:
        if args.elink_qa:
            selected_creds['osti_api'] = creds.osti_v2_qa
        else:
            selected_creds['osti_api'] = creds.osti_v2_prod

    return selected_creds


def get_ssh_server(args, ssh_creds):
    if args.tunnel_needed is False:
        return False

    else:
        print("Opening SSH tunnel.")
        from sshtunnel import SSHTunnelForwarder

        try:
            server = SSHTunnelForwarder(
                ssh_creds['host'],
                ssh_username=ssh_creds['username'],
                # ssh_pkey=(os.path.expanduser("~") + "/.ssh/id_rsa"),
                # allow_agent automatically locates the appropriate ssh key
                allow_agent=True,
                remote_bind_address=ssh_creds['remote'],
                local_bind_address=ssh_creds['local'])

            server.start()
            return server

        except Exception as e:
            print(e)
            exit(1)
