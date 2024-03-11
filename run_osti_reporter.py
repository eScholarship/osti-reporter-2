# OSTI eLink documentation https://review.osti.gov/elink2api/
# External libraries
from pprint import pprint
import requests

# This script requires a "creds.py" in its directory.
# See "creds_template.py" for the required format.
import creds
import args_and_creds
import eschol_db_functions
import elements_db_functions
import transform_pubs_v1  # TK can remove this when E-link v.2 goes live.
import transform_pubs_v2
import test_output

# -----------------------------
# Global vars
submission_limit = 200
submission_count = 0
sql_creds, api_creds, osti_creds, mysql_creds = {}, {}, {}, {}
osti_v1_pubs = []

submission_api_url = "https://review.osti.gov/elink2api/records/submit"

# -----------------------------
# Process and validate arguments
args = args_and_creds.process_args()


# ========================================
def main():

    # Returns an ssh server if needed, otherwise None.
    ssh_server = assign_creds_and_get_tunnel()

    # Get the data from the eschol_osti db
    osti_eschol_db_pubs = eschol_db_functions.get_osti_db(mysql_creds)

    # Get the publications which need to be sent
    new_osti_pubs = elements_db_functions.get_new_osti_pubs(sql_creds, osti_eschol_db_pubs, args)

    if not new_osti_pubs:
        print("No new OSTI publications were found. Exiting.")
        exit(0)

    if args.test:
        test_output.output_elements_query_results(new_osti_pubs)

    # Add OSTI-specific metadata
    if args.elink_version == 1:
        new_osti_pubs = transform_pubs_v1.add_osti_data_v1(new_osti_pubs, args.test)
    elif args.elink_version == 2:
        new_osti_pubs = transform_pubs_v2.add_osti_data_v2(new_osti_pubs, args.test)

    # Output files if using test mode.
    if args.test:
        test_output.output_submissions(new_osti_pubs, args.elink_version)

    # Otherwise, send the jsons or xmls to the osti API.
    else:
        new_osti_pubs = call_osti_api(new_osti_pubs, args.elink_version)
        process_responses(new_osti_pubs)

    # Close SSH tunnel if needed
    if ssh_server:
        ssh_server.stop()

    print("Program complete. Exiting.")


# =======================================
# TK TK send the response data (e.g. osti_id) to our database --> Elements pub record eventually.
def process_responses(new_osti_pubs_with_responses):
    print("PLACEHOLDER for updating the osti_eschol db. Response status codes and jsons:")
    for pub in new_osti_pubs_with_responses:
        pprint(pub['response_status_code'])
        pprint(pub['response_json'])


# =======================================
# Loop the publications, send the XML or JSON, and adds the response to the dict.
def call_osti_api(new_osti_pubs, elink_version):
    print("\n", len(new_osti_pubs), "new publications for submission.")

    # Elink 1 submissions ----------------------
    if elink_version == 1:

        for i, osti_pub in enumerate(new_osti_pubs, 1):

            # TK TK elink v1 testing goes here.
            print(osti_pub)
            exit()

            if i == submission_limit:
                print("Submission limit hit. Breaking submission loop.")
                break

            print("Submitting:", i, ": Publication ID: ", osti_pub['id'])

            # Build the request
            req_url = osti_creds['token'] + "/records/submit"
            headers = {'Content-type': 'text/xml'}
            auth_data = (osti_creds['username'], osti_creds['password'])

            response = requests.post(
                req_url,
                data=osti_pub['submission_xml_string'],
                headers=headers,
                auth=auth_data)

            # Save the response data
            osti_pub['response_status_code'] = response.status_code
            osti_pub['response_json'] = response.json()
            pprint(response)

            if response.status_code >= 300:
                print("\nResponse status code > 300...")
                osti_pub['response_success'] = False

            else:
                print("\nSubmission OK.")
                osti_pub['response_success'] = True
                pprint(response)

        pass  # TK

    # Elink 2 submissions ----------------------
    elif elink_version == 2:

        for i, osti_pub in enumerate(new_osti_pubs, 1):

            if i == submission_limit:
                print("Submission limit hit. Breaking submission loop.")
                break

            print("Submitting:", i, ": Publication ID: ", osti_pub['id'])

            req_url = osti_creds['base_url'] + "/records/submit"
            headers = {'Authorization': 'Bearer ' + osti_creds['token']}
            response = requests.post(
                req_url,
                json=osti_pub['submission_json'],
                headers=headers)

            # Save the response data
            osti_pub['response_status_code'] = response.status_code
            osti_pub['response_json'] = response.json()

            if response.status_code >= 300:
                print("Response status code > 300...\n")
                osti_pub['response_success'] = False

                # for testing
                print(response)
                pprint(response.json())
                pprint(osti_pub['submission_json'])

            else:
                print("Submission OK.\n")
                osti_pub['response_success'] = True

    return new_osti_pubs


# =======================================
# Vars and creds setup
def assign_creds_and_get_tunnel():
    global sql_creds, api_creds, osti_creds, mysql_creds

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

    # If tunnel is not required, return false.
    return False


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
