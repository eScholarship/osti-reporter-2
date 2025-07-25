# Program modules
import program_setup
import write_logs
import cdl_osti_db_functions as cdl
import elements_db_functions as elements
import transform_pubs
import elink_2_functions as elink_2


# Global vars
submission_limit = 200
sleep_time = 3


# =======================================
def main():
    # These lists are populated during the submission process.
    new_osti_pubs = None
    new_osti_pdfs = None
    osti_metadata_updates = None
    osti_pdf_updates = None

    # ---------- GENERAL SETUP
    # Process args; Assign creds based on args; Create the log folder.
    args = program_setup.process_args()
    creds = program_setup.assign_creds(args)
    log_folder = write_logs.create_log_folder()

    # Returns an open & running ssh server if needed, otherwise False.
    ssh_server = program_setup.get_ssh_server(args, creds['ssh'])

    # Gets the db connections for Elements
    elements_conn = elements.get_elements_connection(creds['elements_reporting_db'])

    # CDL OSTI DB --> Elements temp table
    create_and_transfer_temp_table(args, creds, elements_conn, log_folder)

    if not args.updates_only:
        new_osti_pubs = process_new_osti_pubs(
            args, creds, elements_conn, log_folder)

    if args.metadata_updates or args.individual_updates:
        osti_metadata_updates = process_metadata_updates(
            args, creds, elements_conn, log_folder)

    if args.pdf_updates or args.individual_updates:
        osti_pdf_updates = process_pdf_updates(
            args, creds, elements_conn, log_folder)

    # Prints a digest of completed work
    write_logs.print_final_report(
        new_osti_pubs, new_osti_pdfs, osti_metadata_updates, osti_pdf_updates)

    # Close connections.
    elements_conn.close()
    if ssh_server:
        ssh_server.stop()

    print("\nProgram complete. Exiting.\n\n")


# =======================================
def create_and_transfer_temp_table(args, creds, elements_conn, log_folder):
    # Get the data from the CDL OSTI DB
    cdl_osti_db_pubs = cdl.get_cdl_osti_db(creds['cdl_db_read'])

    # Create temp table in Elements
    elements.create_temp_table_in_elements(elements_conn, cdl_osti_db_pubs)

    if args.full_logging:
        temp_table_results = elements.get_full_temp_table(elements_conn)
        write_logs.output_temp_table_results(log_folder, temp_table_results)


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
    if args.full_logging:
        write_logs.output_elements_query_results(log_folder, new_osti_pubs)

    # Add the OSTI-specific submission JSONs
    new_osti_pubs = transform_pubs.add_osti_data(new_osti_pubs, args.test)

    # Log transformed submissions
    if args.full_logging:
        write_logs.output_submissions(log_folder, new_osti_pubs)

    # If running in test mode, skip the submission step.
    if args.test:
        print("Run with test output only. Exiting.")
        elements_conn.close()
        exit(0)

    # Otherwise, send the submission jsons to the OSTI API.
    new_osti_pubs = elink_2.submit_new_pubs(
        new_osti_pubs, creds['osti_api'], creds['cdl_db_write'])

    # Output pub objects with responses
    write_logs.output_json_generic(
        log_folder, new_osti_pubs, "submissions-and-responses")

    # Report successful meta & media submission counts
    meta_ok = len([pub for pub in new_osti_pubs
                   if pub.get('response_success') is True])

    media_ok = len([pub for pub in new_osti_pubs
                    if pub.get('media_response_success') is True])

    print(f"{meta_ok}/{len(new_osti_pubs)} successful metadata submission in this batch.")
    print(f"{media_ok}/{meta_ok} successful media submissions for new metadata.")

    return new_osti_pubs


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
    osti_metadata_updates = transform_pubs.add_osti_data(osti_metadata_updates, args.test)

    # Log metadata updates
    if args.full_logging:
        write_logs.output_submissions(
            log_folder, osti_metadata_updates, "UPDATE-METADATA")

    # Submit updated metadata to OSTI
    osti_metadata_updates = elink_2.submit_metadata_updates(
        osti_metadata_updates, creds['osti_api'], creds['cdl_db_write'])

    # Log OSTI API responses; Output pub objects with responses
    write_logs.output_json_generic(
        log_folder, osti_metadata_updates, "v2-update-submissions-and-responses")

    # Report succesfull metadata updates
    successful_metadata_updates = len([
        pub for pub in osti_metadata_updates if pub.get('response_success') is True])

    print(f"{successful_metadata_updates}/{len(osti_metadata_updates)} "
          f"metadata updates processed successfully.")

    return osti_metadata_updates


# =======================================
# PDF updates
def process_pdf_updates(args, creds, elements_conn, log_folder):

    print("\nQuerying for replaced PDF files.")
    osti_media_updates = elements.get_osti_media_updates(elements_conn, args)

    if not osti_media_updates:
        print("No updated PDFs for resubmission. Proceeding.")
        return False

    print(f"\n{len(osti_media_updates)} Modified media files for updating.")
    print("\nSubmitting updated PDFs to OSTI.")
    elink_2.submit_media_updates(osti_media_updates, creds['osti_api'], creds['cdl_db_write'])

    return osti_media_updates


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
