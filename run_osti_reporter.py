# OSTI eLink documentation https://review.osti.gov/elink2api/

# ========================================
# External libraries

# This script requires a "creds.py" in its directory.
# See "creds_template.py" for the required format.
import program_setup
import write_logs
import eschol_db_functions as eschol
import elements_db_functions as elements
import transform_pubs_v1    # Can remove this when E-link v2 goes live.
import submit_pubs_v1       # Can remove this when E-link v2 goes live.
import transform_pubs_v2
import submit_pubs_v2 as elink_2


# ========================================
# Global vars
submission_limit = 200


# ========================================
def main():

    # Process args, (exit if invalid); Assign creds based on args
    args = program_setup.process_args()
    creds = program_setup.assign_creds(args)

    # Returns an open/running ssh server if needed, otherwise False.
    ssh_server = program_setup.get_ssh_server(args, creds['ssh'])

    # Creates the log folder
    log_folder = write_logs.create_log_folder()

    # Gets the db connections for Elements
    elements_conn = elements.get_elements_connection(creds['elements_reporting_db'])

    # Get the data from the eschol_osti db
    osti_eschol_db_pubs = eschol.get_eschol_osti_db(creds['eschol_db_read'])

    # Create the temp table .SQL; log the SQL; Create temp table in Elements
    temp_table_query = elements.generate_temp_table_sql(osti_eschol_db_pubs)
    write_logs.output_temp_table_query(log_folder, temp_table_query)
    elements.create_temp_table_in_elements(elements_conn, temp_table_query, log_folder)

    # ---------- FIND AND SEND NEW ITEMS
    if not args.test_updates:
        new_osti_pubs = handle_new_osti_pubs(args, creds, elements_conn, log_folder)

    # ---------- METADATA & PDF UPDATES
    if args.elink_version == 2 and args.send_updates:
        osti_metadata_updates = handle_metadata_updates(args, creds, elements_conn, log_folder)
        osti_pdf_updates = handle_pdf_updates(args, creds, elements_conn, log_folder)

    # Close elements db connections
    elements_conn.close()

    # Close SSH tunnel if needed
    if ssh_server:
        ssh_server.stop()

    print("Program complete. Exiting.")


# =======================================
# New OSTI Pubs
def handle_new_osti_pubs(args, creds, elements_conn, log_folder):
    print("Querying for new OSTI publications.")
    new_osti_pubs = elements.get_new_osti_pubs(elements_conn, args)

    if not new_osti_pubs:
        print("No new OSTI publications were found. Proceeding.")

    else:
        # Log Elements query results
        write_logs.output_elements_query_results(log_folder, new_osti_pubs)

        # Add OSTI-specific metadata
        if args.elink_version == 1:
            new_osti_pubs = transform_pubs_v1.add_osti_data_v1(new_osti_pubs, args.test)

        elif args.elink_version == 2:
            new_osti_pubs = transform_pubs_v2.add_osti_data_v2(new_osti_pubs, args.test)

        # Log submission files
        if not new_osti_pubs:
            print("No new pubs for submission. Proceeding.")
            return False

        print(f"\n{len(new_osti_pubs)} new pubs for submission.")
        write_logs.output_submissions(log_folder, new_osti_pubs, args.elink_version)

        # If running in test mode, skip the submission step.
        if args.test:
            print("Run with test output only. Exiting.")
            elements_conn.close()
            exit(0)

        # Otherwise, send the xml (v1) or json (v2) submissions to the OSTI API.
        if args.elink_version == 1:
            new_osti_pubs = submit_pubs_v1.submit_pubs(new_osti_pubs, creds['osti_api'], submission_limit)

        elif args.elink_version == 2:
            new_osti_pubs = elink_2.submit_pubs(new_osti_pubs, creds['osti_api'], submission_limit)

        # Log OSTI API responses
        write_logs.output_responses(log_folder, new_osti_pubs, args.elink_version)

        # Output pub objects with responses
        if args.elink_version == 2:
            write_logs.output_json_generic(log_folder, new_osti_pubs, args.elink_version, "v2-responses")

        # Update eSchol OSTI DB with new successful submissions
        successful_submissions = [pub for pub in new_osti_pubs if pub['response_success'] is True]
        if len(successful_submissions) == 0:
            print("No successful submissions in this set of publications. Proceeding.")
        else:
            eschol.update_eschol_osti_db(successful_submissions, creds['eschol_db_write'])

        return new_osti_pubs


# =======================================
# Metadata updates
def handle_metadata_updates(args, creds, elements_conn, log_folder):
    print("Querying for modified OSTI pubs.")
    osti_metadata_updates = elements.get_osti_metadata_updates(elements_conn, args)

    if not osti_metadata_updates:
        print("No OSTI pubs with modified metadata. Proceeding.")
        return False

    else:
        print("\n", len(osti_metadata_updates), "Modified publications for updating.")

        # Transform metadata updates for submission
        osti_metadata_updates = transform_pubs_v2.add_osti_data_v2(osti_metadata_updates, args.test)

        # Log metadata updates
        write_logs.output_submissions(
            log_folder, osti_metadata_updates, args.elink_version, "UPDATE-METADATA")

        # Submit updated metadata to OSTI
        osti_metadata_updates = elink_2.submit_metadata_updates(
            osti_metadata_updates, creds['osti_api'], submission_limit)

        ## TK TK write the updates

        # Log OSTI API responses
        write_logs.output_responses(log_folder, osti_metadata_updates, args.elink_version)

        # Output pub objects with responses
        write_logs.output_json_generic(log_folder, osti_metadata_updates, args.elink_version, "v2-update-responses")

        return osti_metadata_updates


# =======================================
# PDF updates
def handle_pdf_updates(args, creds, elements_conn, log_folder):
    return True
    # TK TK pick up here.
    print("Querying for replaced PDF files.")

    # Get any OSTI pubs with updated metadata
    osti_media_updates = elements.get_osti_media_updates(elements_conn, args)

    if not osti_media_updates:
        print("No replaced PDFs to resubmit. Proceeding.")

    else:
        print(osti_media_updates)

    return osti_media_updates


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
