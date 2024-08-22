# OSTI eLink documentation https://review.osti.gov/elink2api/

# Program modules
import program_setup
import write_logs
import eschol_db_functions as eschol
import elements_db_functions as elements
import transform_pubs
import submit_pubs as elink_2

# Global vars
submission_limit = 200
sleep_time = 3


def main():

    # These lists are populated during the submission process.
    new_osti_pubs, new_osti_pdfs, osti_metadata_updates, osti_pdf_updates = None, None, None, None

    # ---------- GENERAL SETUP
    # Process args; Assign creds based on args; Create the log folder.
    args = program_setup.process_args()
    creds = program_setup.assign_creds(args)
    log_folder = write_logs.create_log_folder()

    # Returns an open & running ssh server if needed, otherwise False.
    ssh_server = program_setup.get_ssh_server(args, creds['ssh'])

    # Gets the db connections for Elements
    elements_conn = elements.get_elements_connection(creds['elements_reporting_db'])

    # eSchol OSTI DB --> Elements temp table
    transfer_temp_table(creds, elements_conn, log_folder)

    # ---------- E-Link 1
    if args.elink_version == 1 and not args.test_updates:
        new_osti_pubs = process_elink_1_pubs(args, creds, elements_conn, log_folder)

    # ---------- E-Link 2
    elif args.elink_version == 2:
        if not args.test_updates:
            new_osti_pubs = process_new_osti_pubs(args, creds, elements_conn, log_folder)
            new_osti_pdfs = process_new_osti_pdfs(args, creds, elements_conn, log_folder, new_osti_pubs)

        if args.send_updates:
            osti_metadata_updates = process_metadata_updates(args, creds, elements_conn, log_folder)
            osti_pdf_updates = process_pdf_updates(args, creds, elements_conn, log_folder)

    # Prints a digest of completed work
    print_final_report(new_osti_pubs, new_osti_pdfs, osti_metadata_updates, osti_pdf_updates)

    # Close connections.
    elements_conn.close()
    if ssh_server:
        ssh_server.stop()

    print("\nProgram complete. Exiting.\n\n")


# =======================================
def transfer_temp_table(creds, elements_conn, log_folder):
    # Get the data from the eschol_osti db
    osti_eschol_db_pubs = eschol.get_eschol_osti_db(creds['eschol_db_read'])

    # Create the temp table .SQL, and log it
    temp_table_query = elements.generate_temp_table_sql(osti_eschol_db_pubs)
    write_logs.output_temp_table_query(log_folder, temp_table_query)

    # Create temp table in Elements
    elements.create_temp_table_in_elements(elements_conn, temp_table_query, log_folder)


# =======================================
# E-Link 1
def process_elink_1_pubs(args, creds, elements_conn, log_folder):
    import transform_pubs_v1  # Can remove this when E-link v2 goes live.
    import submit_pubs_v1 as elink_1  # Can remove this when E-link v2 goes live.

    print("\nQuerying Elements Reporting DB for new OSTI publications.")
    new_osti_pubs = elements.get_new_osti_pubs(elements_conn, args)

    if not new_osti_pubs:
        print("No new OSTI publications were found. Proceeding.")
        return False

    print(f"\n{len(new_osti_pubs)} new pubs for submission.")
    if len(new_osti_pubs) > submission_limit:
        print(f"Truncating new pub list to submission limit ({submission_limit})")
        new_osti_pubs = new_osti_pubs[submission_limit:]

    # Log Elements query results
    write_logs.output_elements_query_results(log_folder, new_osti_pubs)

    # Add the OSTI-specific submission XMLs
    new_osti_pubs = transform_pubs_v1.add_osti_data_v1(new_osti_pubs, args.test)

    # Log transformed submissions
    write_logs.output_submissions(log_folder, new_osti_pubs, args.elink_version)

    # If running in test mode, skip the submission step.
    if args.test:
        print("Run with test output only. Exiting.")
        elements_conn.close()
        exit(0)

    # Otherwise, send the submission XMLs to the OSTI API.
    new_osti_pubs = elink_1.submit_pubs(new_osti_pubs, creds['osti_api'], submission_limit)

    # Output pub objects with responses
    write_logs.output_json_generic(log_folder, new_osti_pubs, args.elink_version, "v1-responses")

    # Update eSchol OSTI DB with new successful submissions
    successful_submissions = [pub for pub in new_osti_pubs if pub['response_success'] is True]

    if len(successful_submissions) == 0:
        print("\nNo successful metadata submissions in this set. Proceeding.")
    else:
        print("\nAdding new successful metadata submissions to eSchol OSTI DB:")
        eschol.insert_new_metadata_submissions(successful_submissions, creds['eschol_db_write'])

    return new_osti_pubs


# =======================================
# New OSTI Pubs
def process_new_osti_pubs(args, creds, elements_conn, log_folder):

    print("\nQuerying Elements Reporting DB for new OSTI publications.")
    new_osti_pubs = elements.get_new_osti_pubs(elements_conn, args)

    if not new_osti_pubs:
        print("No new OSTI publications were found. Proceeding.")
        return False

    print(f"\n{len(new_osti_pubs)} new pubs for submission.")
    if len(new_osti_pubs) > submission_limit:
        print(f"Truncating new pub list to submission limit ({submission_limit})")
        new_osti_pubs = new_osti_pubs[submission_limit:]

    # Log Elements query results
    write_logs.output_elements_query_results(log_folder, new_osti_pubs)

    # Add the OSTI-specific submission JSONs
    new_osti_pubs = transform_pubs.add_osti_data_v2(new_osti_pubs, args.test)

    # Log transformed submissions
    write_logs.output_submissions(log_folder, new_osti_pubs, args.elink_version)

    # If running in test mode, skip the submission step.
    if args.test:
        print("Run with test output only. Exiting.")
        elements_conn.close()
        exit(0)

    # Otherwise, send the submission jsons to the OSTI API.
    new_osti_pubs = elink_2.submit_new_pubs(new_osti_pubs, creds['osti_api'])

    # Log OSTI API responses -- TK this is handy for testing but can be removed
    # write_logs.output_responses(log_folder, new_osti_pubs, args.elink_version)

    # Output pub objects with responses
    write_logs.output_json_generic(log_folder, new_osti_pubs, args.elink_version, "v2-responses")

    # Update eSchol OSTI DB with new successful submissions
    successful_submissions = [
        pub for pub in new_osti_pubs if pub['response_success'] is True]

    if len(successful_submissions) == 0:
        print("\nNo successful metadata submissions in this set. Proceeding.")
    else:
        print("\nAdding new successful metadata submissions to eSchol OSTI DB:")
        eschol.insert_new_metadata_submissions(successful_submissions, creds['eschol_db_write'])

    return new_osti_pubs


# =======================================
# New PDFs
def process_new_osti_pdfs(args, creds, elements_conn, log_folder, new_osti_pubs):
    successful_new_pubs = [p for p in new_osti_pubs if p['response_success']]

    if not successful_new_pubs:
        print("\nNo successful submissions, so we're not sending any PDFs.")
        return False

    print("\nSubmitting PDFs for successfully-added new pubs.")
    new_osti_pubs_with_pdf_responses = elink_2.submit_new_pdfs(successful_new_pubs, creds['osti_api'])

    print("\nUpdating eSchol OSTI DB with media responses"
          "\n(Note: Failure codes will be saved to the eSchol DB if received)")
    eschol.update_new_submissions_with_pdfs(new_osti_pubs_with_pdf_responses, creds['eschol_db_write'])

    return new_osti_pubs_with_pdf_responses


# =======================================
# Metadata updates
def process_metadata_updates(args, creds, elements_conn, log_folder):

    print("\nQuerying for modified OSTI pubs.")
    osti_metadata_updates = elements.get_osti_metadata_updates(elements_conn, args)

    if not osti_metadata_updates:
        print("No OSTI pubs with modified metadata. Proceeding.")
        return False

    print(f"\n{len(osti_metadata_updates)} Modified publications for updating.")
    if len(osti_metadata_updates) > submission_limit:
        print(f"Truncating new pub list to submission limit ({submission_limit})")
        osti_metadata_updates = osti_metadata_updates[submission_limit:]

    # Transform metadata updates for submission
    osti_metadata_updates = transform_pubs.add_osti_data_v2(osti_metadata_updates, args.test)

    # Log metadata updates
    write_logs.output_submissions(
        log_folder, osti_metadata_updates, args.elink_version, "UPDATE-METADATA")

    # Submit updated metadata to OSTI
    osti_metadata_updates = elink_2.submit_metadata_updates(osti_metadata_updates, creds['osti_api'])

    # Log OSTI API responses; Output pub objects with responses
    write_logs.output_json_generic(
        log_folder, osti_metadata_updates, args.elink_version, "v2-update-responses")

    # Update eSchol OSTI DB with new successful submissions
    successful_metadata_updates = [
        pub for pub in osti_metadata_updates if pub['response_success'] is True]

    if len(successful_metadata_updates) == 0:
        print("No successful metadata updates. Proceeding.")
    else:
        print("\nWriting successful metadata updates to to eSchol OSTI DB:")
        eschol.update_osti_db_metadata(successful_metadata_updates, creds['eschol_db_write'])

    return osti_metadata_updates


# =======================================
# PDF updates
def process_pdf_updates(args, creds, elements_conn, log_folder):

    print("\nQuerying for replaced PDF files.")
    osti_media_updates = elements.get_osti_media_updates(elements_conn, args)

    if not osti_media_updates:
        print("No replaced PDFs to resubmit. Proceeding.")
        return False

    # print(osti_media_updates)
    elink_2.submit_media_updates(osti_media_updates, creds['osti_api'], submission_limit)

    # for testing
    return False
    # return osti_media_updates


# =======================================
def print_final_report(new_osti_pubs, new_osti_pdfs, osti_metadata_updates, osti_pdf_updates):

    def print_report_header():
        print("\n\n========================================"
              "\n         OSTI REPORTER: SUMMARY"
              "\n========================================\n")

    def report_builder(pubs, message, success_field, failure_json_field):
        print("\n--------------------")

        if not pubs:
            print(f"No {message}")

        else:
            success = [p for p in pubs if p[success_field]]
            failure = [p for p in pubs if not p[success_field]]

            print(f"{len(pubs)} total {message}")
            if success:
                print(f"{len(success)} successful submissions (Elements IDs):")
                for s in success:
                    print(s['id'])
            if failure:
                print(f"\n{len(failure)} failed submissions:")
                for f in failure:
                    print(f"\n{f['id']}")
                    print(f[failure_json_field])

    print_report_header()

    report_builder(new_osti_pubs, "new pubs sent to OSTI.",
                   'response_success', 'response_json')

    report_builder(new_osti_pdfs, "PDFs submitted for newly-added publications.",
                   'media_response_success', 'media_response_json')

    report_builder(osti_metadata_updates, "pubs with updated metadata sent to OSTI.",
                   'response_success', 'response_json')

    report_builder(osti_pdf_updates, "replacement PDFs sent to OSTI.",
                   'media_response_success', 'media_response_json')

    print_report_header()


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
