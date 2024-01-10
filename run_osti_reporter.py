# OSTI eLink documentation https://review.osti.gov/elink2api/

# This script requires a "creds.py" in its directory.
# See "creds_template.py" for the required format.
import creds
import transform_pubs_v1 # TK can remove this when E-link v.2 goes live.
import transform_pubs_v2

# External libraries
import argparse
import pyodbc
import datetime
from pprint import pprint
import requests

# TK can delete this probably
# import xml.etree.ElementTree as ET

# -----------------------------
# Global vars
submission_limit = 200
submission_count = 0
sql_creds, api_creds, osti_creds, mysql_creds = {}, {}, {}, {}
osti_v1_pubs = []

submission_api_url = "https://review.osti.gov/elink2api/records/submit"

# -----------------------------
# Arguments
parser = argparse.ArgumentParser()

parser.add_argument("-c", "--connection",
                    dest="connection",
                    type=str.lower,
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
                    type=int,
                    default=2,
                    help="Specify OSTI elink version 1 or 2 (default)")

parser.add_argument("-x", "--test",
                    dest="test",
                    action="store_true",
                    default=False,
                    help="Outputs update XML or JSON to disk rather than sending to OSTI API.")

args = parser.parse_args()


# ========================================
def main():
    global sql_creds, api_creds, osti_creds, mysql_creds

    # Validate args
    if (args.connection == 'qa' or args.connection == 'production') \
            and (args.elink_version == 1 or args.elink_version == 2):
        pass
    else:
        print("Invalid arguments provided. See here:")
        print(parser.print_help())
        exit(0)

    # Loads creds based on the above flags
    # --------- QA
    if args.connection == 'qa':
        ssh_creds = creds.ssh_creds_qa
        # api_creds = creds.api_creds_qa
        mysql_creds = creds.mysql_creds
        if args.tunnel_needed:
            sql_creds = creds.sql_creds_local_qa
        else:
            sql_creds = creds.sql_creds_server_qa

    # --------- PROD
    else:
        ssh_creds = creds.ssh_creds_prod
        # api_creds = creds.api_creds_prod
        mysql_creds = creds.mysql_creds
        if args.tunnel_needed:
            sql_creds = creds.sql_creds_local_prod
        else:
            sql_creds = creds.sql_creds_server_prod

    # --------- OSTI ELINK VERSION
    if args.elink_version == 1:
        osti_creds = creds.osti_v1_creds
    else:
        osti_creds = creds.osti_v2_creds

    # Open SSH tunnel if needed
    if args.tunnel_needed:
        print("Opening SSH tunnel.")

        # SSH-specific packages
        from sshtunnel import SSHTunnelForwarder
        # import paramiko
        # import os

        # CDL is now using RSA keys, we have to read the file to SSH in.
        # os.chdir(os.path.expanduser("~"))
        # rsa_key = paramiko.RSAKey.from_private_key_file(ssh_creds['key_location'])

        # Connect using RSA key
        server = SSHTunnelForwarder(
            ssh_creds['host'],
            ssh_username=ssh_creds['username'],
            #ssh_pkey=rsa_key,
            ssh_password=ssh_creds['password'],
            remote_bind_address=ssh_creds['remote'],
            local_bind_address=ssh_creds['local'])

        server.start()

    # ----------------------------
    # Get the publications which need to be sent
    new_osti_pubs = get_new_osti_pubs(sql_creds, args.elink_version)

    if new_osti_pubs == []:
        print("No new OSTI publications were found. Exiting.")
        exit(0)

    # Add OSTI-specific metadata
    if args.elink_version == 1:
        new_osti_pubs = transform_pubs_v1.add_osti_data_v1(new_osti_pubs, args.test)
    elif args.elink_version == 2:
        new_osti_pubs = transform_pubs_v2.add_osti_data_v2(new_osti_pubs, args.test)

    # Call OSTI API or output to files
    if args.test:
        output_test_files(new_osti_pubs, args.elink_version)
    else:
        call_osti_api(new_osti_pubs, args.elink_version)

    # ----------------------------
    # Close SSH tunnel if needed
    if args.tunnel_needed:
        server.stop()

    print("Program complete. Exiting.")


# ========================================
# Query the Elements DB, find pubs which need to be sent.
def get_new_osti_pubs(sql_creds, elink_version):

    # Load SQL file
    try:
        sql_file = open("get_new_osti_pubs_with_json.sql")
        sql_query = sql_file.read()
    except:
        raise Exception("ERROR WHILE HANDLING SQL FILE. The file was unable to be located, \
                or a problem occurred while reading its contents.")

    # Connect to db
    try:
        conn = pyodbc.connect(
            driver=sql_creds['driver'],
            server=(sql_creds['server'] + ',' + sql_creds['port']),
            database=sql_creds['database'],
            uid=sql_creds['user'],
            pwd=sql_creds['password'],
            trustservercertificate='yes')

    except:
        raise Exception("ERROR CONNECTING TO DATABASE. Check credits and/or SSH tunneling.")

    # Create cursor, execute query
    print("Connected to Elements reporting DB. Sending Query.")
    conn.autocommit = True  # Required when queries use TRANSACTION
    cursor = conn.cursor()
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


# -----------------------------
# Outputs test files
def output_test_files(new_osti_pubs, elink_version):

    if elink_version == 1:
        for index, osti_pub_xml_string in enumerate(new_osti_pubs):
            filename = "v1-test-" + str(index)
            with open("test_output/v1/" + filename + ".xml", "wb") as out_file:
                out_file.write(osti_pub_xml_string)

    elif elink_version == 2:
        for index, osti_pub_json_string in enumerate(new_osti_pubs):
            filename = "v2-test-" + str(index)
            with open("test_output/v2/" + filename + ".json", "w") as out_file:
                out_file.write(osti_pub_json_string)


# -----------------------------
# Loop the publications, send the XML or JSON

def call_osti_api(new_osti_pubs, elink_version):

    if elink_version == 1:
        pass  # TK

    elif elink_version == 2:
        for i in range(22, 28):
            req_url = osti_creds['base_url'] + "/records/submit"
            headers = {'Authorization': 'Bearer ' + osti_creds['token']}
            pub_json = new_osti_pubs[i]
            response = requests.post(req_url, json=pub_json, headers=headers)

            if response.status_code >= 300:
                print(response)
                pprint(response.json())
                pprint(pub_json)


# -----------------------------
# Stub for main
if __name__ == "__main__":
    main()
