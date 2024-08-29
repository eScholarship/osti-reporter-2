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

    return parser.parse_args()


def assign_creds(args):
    import os
    from dotenv import load_dotenv
    load_dotenv()

    selected_creds = {}

    # Elements reporting db MSSQL
    if args.input_qa:
        selected_creds['elements_reporting_db'] = {
            'user': os.environ['ELEMENTS_REPORTING_DB_QA_USER'],
            'password': os.environ['ELEMENTS_REPORTING_DB_QA_PASSWORD'],
            'server': os.environ['ELEMENTS_REPORTING_DB_QA_SERVER'],
            'port': os.environ['ELEMENTS_REPORTING_DB_QA_PORT'],
            'database': os.environ['ELEMENTS_REPORTING_DB_QA_DATABASE'],
            'driver': os.environ['ELEMENTS_REPORTING_DB_QA_DRIVER']
        }

    else:
        selected_creds['elements_reporting_db'] = {
            'user': os.environ['ELEMENTS_REPORTING_DB_PROD_USER'],
            'password': os.environ['ELEMENTS_REPORTING_DB_PROD_PASSWORD'],
            'server': os.environ['ELEMENTS_REPORTING_DB_PROD_SERVER'],
            'port': os.environ['ELEMENTS_REPORTING_DB_PROD_PORT'],
            'database': os.environ['ELEMENTS_REPORTING_DB_PROD_DATABASE'],
            'driver': os.environ['ELEMENTS_REPORTING_DB_PROD_DRIVER']
        }

    import creds

    # SSH tunnel
    if args.input_qa:
        selected_creds['ssh'] = {
            'host': os.environ['SSH_QA_HOST'],
            'username': os.environ['SSH_QA_USERNAME'],
            'password': os.environ['SSH_QA_PASSWORD'],
            'remote': (os.environ['SSH_QA_REMOTE_URL'],
                       os.environ['SSH_QA_REMOTE_PORT']),
            'local': (os.environ['SSH_QA_LOCAL_URL'],
                      os.environ['SSH_QA_LOCAL_PORT'])
        }

    else:
        selected_creds['ssh'] = {
            'host': os.environ['SSH_PROD_HOST'],
            'username': os.environ['SSH_PROD_USERNAME'],
            'password': os.environ['SSH_PROD_PASSWORD'],
            'key_location': os.environ['SSH_PROD_KEY_LOCATION'],
            'remote': (os.environ['SSH_PROD_REMOTE_URL'],
                       os.environ['SSH_PROD_REMOTE_PORT']),
            'local': (os.environ['SSH_PROD_LOCAL_URL'],
                      os.environ['SSH_PROD_LOCAL_PORT'])
        }

    # Elements reporting db MSSQL
    # if args.input_qa:
    #     if args.tunnel_needed:
    #         selected_creds['elements_reporting_db'] = creds.elements_reporting_db_local_qa
    #     else:
    #         selected_creds['elements_reporting_db'] = creds.elements_reporting_db_server_qa
    # else:
    #     if args.tunnel_needed:
    #         selected_creds['elements_reporting_db'] = creds.elements_reporting_db_local_prod
    #     else:
    #         selected_creds['elements_reporting_db'] = creds.elements_reporting_db_server_prod

    # eSchol MySQL for input (read)
    if args.input_qa:
        selected_creds['eschol_db_read'] = {
            'host': os.environ['ESCHOL_OSTI_DB_QA_SERVER'],
            'database': os.environ['ESCHOL_OSTI_DB_QA_DATABASE'],
            'user': os.environ['ESCHOL_OSTI_DB_QA_USER'],
            'password': os.environ['ESCHOL_OSTI_DB_QA_PASSWORD'],
            'table': os.environ['ESCHOL_OSTI_DB_QA_TABLE']
        }

    else:
        selected_creds['eschol_db_read'] = {
            'host': os.environ['ESCHOL_OSTI_DB_PROD_SERVER'],
            'database': os.environ['ESCHOL_OSTI_DB_PROD_DATABASE'],
            'user': os.environ['ESCHOL_OSTI_DB_PROD_USER'],
            'password': os.environ['ESCHOL_OSTI_DB_PROD_PASSWORD'],
            'table': os.environ['ESCHOL_OSTI_DB_PROD_TABLE']
        }

    # eSchol MySQL for output (write)
    if args.output_qa:
        selected_creds['eschol_db_write'] = {
            'host': os.environ['ESCHOL_OSTI_DB_QA_SERVER'],
            'database': os.environ['ESCHOL_OSTI_DB_QA_DATABASE'],
            'user': os.environ['ESCHOL_OSTI_DB_QA_USER'],
            'password': os.environ['ESCHOL_OSTI_DB_QA_PASSWORD'],
            'table': os.environ['ESCHOL_OSTI_DB_QA_TABLE']
        }

    else:
        selected_creds['eschol_db_write'] = {
            'host': os.environ['ESCHOL_OSTI_DB_PROD_SERVER'],
            'database': os.environ['ESCHOL_OSTI_DB_PROD_DATABASE'],
            'user': os.environ['ESCHOL_OSTI_DB_PROD_USER'],
            'password': os.environ['ESCHOL_OSTI_DB_PROD_PASSWORD'],
            'table': os.environ['ESCHOL_OSTI_DB_PROD_TABLE']
        }

    # OSTI Elink
    if args.elink_version == 1:
        if args.elink_qa:
            selected_creds['osti_api'] = {
                "base_url": os.environ['OSTI_V1_QA_URL'],
                "username": os.environ['OSTI_V1_QA_USERNAME'],
                "password": os.environ['OSTI_V1_QA_PASSWORD']
            }
        else:
            selected_creds['osti_api'] = {
                "base_url": os.environ['OSTI_V1_PROD_URL'],
                "username": os.environ['OSTI_V1_PROD_USERNAME'],
                "password": os.environ['OSTI_V1_PROD_PASSWORD']
            }
    else:
        if args.elink_qa:
            selected_creds['osti_api'] = {
                "base_url": os.environ['OSTI_V2_QA_URL'],
                "token": os.environ['OSTI_V2_QA_TOKEN']
            }
        else:
            selected_creds['osti_api'] = {
                "base_url": os.environ['OSTI_V2_PROD_URL'],
                "token": os.environ['OSTI_V2_PROD_TOKEN']
            }

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
