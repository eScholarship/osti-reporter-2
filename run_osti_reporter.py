# OSTI eLink documentation https://review.osti.gov/elink2api/

# ========================================
# External libraries

# This script requires a "creds.py" in its directory.
# See "creds_template.py" for the required format.
import program_setup
import write_logs
import eschol_db_functions
import elements_db_functions
import transform_pubs_v1    # Can remove this when E-link v2 goes live.
import submit_pubs_v1       # Can remove this when E-link v2 goes live.
import transform_pubs_v2
import submit_pubs_v2


# ========================================
# Global vars
submission_limit = 200


# ========================================
def main():

    # Process and validate arguments. Will exit if invalid.
    args = program_setup.process_args()

    # Assign creds based on arg values
    creds = program_setup.assign_creds(args)

    # Returns an open/running ssh server if needed, otherwise False.
    ssh_server = program_setup.get_ssh_server(args, creds['ssh'])

    # Creates the log folder
    log_folder = write_logs.create_log_folder()

    # Gets the db connections for eSchol and Elements
    # eschol_db_conn = eschol_db_functions.get_eschol_connection(creds['eschol_db_read'])
    elements_db_conn = elements_db_functions.get_elements_connection(creds['elements_reporting_db'])

    # Get the data from the eschol_osti db
    osti_eschol_db_pubs = eschol_db_functions.get_eschol_osti_db(creds['eschol_db_read'])

    # Create the temp table for Elements, write to logs
    osti_eschol_temp_table_query = elements_db_functions.create_submitted_temp_table(osti_eschol_db_pubs)

    # Log temp table query
    write_logs.output_temp_table_query(log_folder, osti_eschol_temp_table_query)

    # Get the publications which need to be sent, exit if there's none.
    new_osti_pubs = elements_db_functions.get_new_osti_pubs(
        elements_db_conn, osti_eschol_temp_table_query, args, log_folder)

    if not new_osti_pubs:
        print("No new OSTI publications were found. Exiting.")
        exit(0)

    # Log Elements query results
    write_logs.output_elements_query_results(log_folder, new_osti_pubs)

    # Add OSTI-specific metadata
    if args.elink_version == 1:
        new_osti_pubs = transform_pubs_v1.add_osti_data_v1(new_osti_pubs, args.test)
    elif args.elink_version == 2:
        new_osti_pubs = transform_pubs_v2.add_osti_data_v2(new_osti_pubs, args.test)

    # Log submission files
    write_logs.output_submissions(log_folder, new_osti_pubs, args.elink_version)

    # If running in test mode, skip the submission step.
    if args.test:
        print("\n", len(new_osti_pubs), "new publications -- Test output only. Exiting.")
        exit(0)

    # Otherwise, send the xml (v1) or json (v2) submissions to the OSTI API.
    print("\n", len(new_osti_pubs), "new publications for submission.")

    if args.elink_version == 1:
        new_osti_pubs = submit_pubs_v1.submit_pubs(new_osti_pubs, creds['osti_api'], submission_limit)

    elif args.elink_version == 2:
        new_osti_pubs = submit_pubs_v2.submit_pubs(new_osti_pubs, creds['osti_api'], submission_limit)

    # Log OSTI API responses
    write_logs.output_responses(log_folder, new_osti_pubs, args.elink_version)

    # TK testing work happening here
    if args.elink_version == 2:
        write_logs.output_json_generic(log_folder, new_osti_pubs, args.elink_version, "v2-responses")

    # Update eSchol OSTI DB with new successful submissions
    eschol_db_functions.update_eschol_osti_db(new_osti_pubs, creds['eschol_db_write'])

    # Close elements db connections
    elements_db_conn.close()

    # Close SSH tunnel if needed
    if ssh_server:
        ssh_server.stop()

    print("Program complete. Exiting.")


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
