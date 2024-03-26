# OSTI eLink documentation https://review.osti.gov/elink2api/
# External libraries
from pprint import pprint

# This script requires a "creds.py" in its directory.
# See "creds_template.py" for the required format.
import program_setup
import eschol_db_functions
import elements_db_functions
import transform_pubs_v1  # TK can remove this when E-link v.2 goes live.
import submit_pubs_v1  # TK can remove this when E-link v.2 goes live.
import transform_pubs_v2
import submit_pubs_v2
import test_output

# -----------------------------
# Global vars
submission_limit = 200
submission_count = 0

# submission_api_url = "https://review.osti.gov/elink2api/records/submit"

# ========================================
def main():

    # Process and validate arguments. Will exit if invalid.
    args = program_setup.process_args()

    # Assign creds based on arg values
    creds = program_setup.assign_creds(args)

    # Returns an open/running ssh server if needed, otherwise False.
    ssh_server = program_setup.get_ssh_server(args, creds['ssh'])

    # Get the data from the eschol_osti db
    osti_eschol_db_pubs = eschol_db_functions.get_eschol_osti_db(creds['eschol_db_read'])

    # Get the publications which need to be sent, exit if there's none.
    new_osti_pubs = elements_db_functions.get_new_osti_pubs(creds['elements_reporting_db'], osti_eschol_db_pubs, args)

    if not new_osti_pubs:
        print("No new OSTI publications were found. Exiting.")
        exit(0)

    # Output elements query results if running in test mode
    if args.test:
        test_output.output_elements_query_results(new_osti_pubs)

    # Add OSTI-specific metadata
    if args.elink_version == 1:
        new_osti_pubs = transform_pubs_v1.add_osti_data_v1(new_osti_pubs, args.test)

    elif args.elink_version == 2:
        new_osti_pubs = transform_pubs_v2.add_osti_data_v2(new_osti_pubs, args.test)

    # Output submission files if using test mode.
    if args.test:
        test_output.output_submissions(new_osti_pubs, args.elink_version)

    # Otherwise, send the json or xml submissions to the osti API.
    else:
        print("\n", len(new_osti_pubs), "new publications for submission.")

        if args.elink_version == 1:
            new_osti_pubs = submit_pubs_v1.submit_pubs(new_osti_pubs, creds['osti_api'], submission_limit)
            eschol_db_functions.update_eschol_osti_db(new_osti_pubs, creds['eschol_db_write'])

        elif args.elink_version == 2:
            new_osti_pubs = submit_pubs_v2.submit_pubs(new_osti_pubs, creds['osti_api'], submission_limit)
            eschol_db_functions.update_eschol_osti_db(new_osti_pubs, creds['eschol_db_write'])

    # Close SSH tunnel if needed
    if ssh_server:
        ssh_server.stop()

    print("Program complete. Exiting.")


# =======================================
if __name__ == "__main__":
    main()
