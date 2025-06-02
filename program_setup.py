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

    parser.add_argument("-t", "--tunnel",
                        dest="tunnel_needed",
                        action="store_true",
                        default=False,
                        help="Optional. Include to run the connection through a tunnel.")

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

    parser.add_argument("-xu", "--test-updates",
                        dest="test_updates",
                        action="store_true",
                        default=False,
                        help="Skips ordinary submission step and jumps to update steps.")

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
    from dotenv import dotenv_values
    env = dotenv_values(".env")

    # Arg switches
    input_cnx = "_QA" if args.input_qa else "_PROD"
    elink_cnx = "_QA" if args.elink_qa else "_PROD"
    output_cnx = "_QA" if args.output_qa else "_PROD"

    selected_creds = {}

    # Elements reporting db MSSQL
    selected_creds['elements_reporting_db'] = {
        'user': env['ELEMENTS_REPORTING_DB_USER' + input_cnx],
        'password': env['ELEMENTS_REPORTING_DB_PASSWORD' + input_cnx],
        'server': env['ELEMENTS_REPORTING_DB_SERVER' + input_cnx],
        'port': env['ELEMENTS_REPORTING_DB_PORT' + input_cnx],
        'database': env['ELEMENTS_REPORTING_DB_DATABASE' + input_cnx],
        'driver': env['ELEMENTS_REPORTING_DB_DRIVER' + input_cnx]}

    # SSH tunnel
    selected_creds['ssh'] = {
        'host': env['SSH_HOST' + input_cnx],
        'username': env['SSH_USERNAME' + input_cnx],
        'password': env['SSH_PASSWORD' + input_cnx],
        'remote': (env['SSH_REMOTE_URL' + input_cnx],
                   env['SSH_REMOTE_PORT' + input_cnx]),
        'local': (env['SSH_LOCAL_URL' + input_cnx],
                  env['SSH_LOCAL_PORT' + input_cnx])}

    # eSchol MySQL for input (read)
    selected_creds['eschol_db_read'] = {
        'host': env['ESCHOL_OSTI_DB_SERVER' + input_cnx],
        'database': env['ESCHOL_OSTI_DB_DATABASE' + input_cnx],
        'user': env['ESCHOL_OSTI_DB_USER' + input_cnx],
        'password': env['ESCHOL_OSTI_DB_PASSWORD' + input_cnx],
        'table': env['ESCHOL_OSTI_DB_TABLE' + input_cnx]}

    # eSchol MySQL for output (write)
    selected_creds['eschol_db_write'] = {
        'host': env['ESCHOL_OSTI_DB_SERVER' + output_cnx],
        'database': env['ESCHOL_OSTI_DB_DATABASE' + output_cnx],
        'user': env['ESCHOL_OSTI_DB_USER' + output_cnx],
        'password': env['ESCHOL_OSTI_DB_PASSWORD' + output_cnx],
        'table': env['ESCHOL_OSTI_DB_TABLE' + output_cnx]}

    # OSTI Elink
    selected_creds['osti_api'] = {
        "base_url": env['OSTI_URL' + elink_cnx],
        "token": env['OSTI_TOKEN' + elink_cnx]}

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
                allow_agent=True,
                remote_bind_address=ssh_creds['remote'],
                local_bind_address=ssh_creds['local'])

            server.start()
            return server

        except Exception as e:
            print(e)
            exit(1)
