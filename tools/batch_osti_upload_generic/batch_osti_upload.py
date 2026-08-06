import csv
import sys
import os
import json
from datetime import datetime

# Adds main folder to path for importing standard elink modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import program_setup
from batch_prep_for_transform import prep_row_for_transform
import cdl_osti_db_functions as cdl
import elink_2_functions as elink_2


# globals
input_dir = "input"
output_dir = "output"
submission_sleep_time = 3

submit_metadata = True
submit_media = True

# Batch submission-specific vars
batch_group = "QSA"
metadata_file = "qsa-2.csv"
pdf_dir = "pdfs"
pdf_field = "arXiv (and name of file)"

output_filename_stem = f"{metadata_file}_output"
first_metadata_csv_write, first_media_csv_write = True, True

skip_rows = []


def main():
    # ---------- GENERAL SETUP
    # Process args; Assign creds based on args; Create the log folder.
    args = program_setup.process_args()
    creds = program_setup.assign_creds(args)

    # Create output folder for logging
    run_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    run_output_dir = f"{output_dir}/{run_time}"
    os.mkdir(run_output_dir)

    # Load metadata (utf-8-sig is for reading xlsx-exported CSVs)
    with open(f"{input_dir}/{metadata_file}", 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader if r[pdf_field]]
        print(f"{len(rows)} metadata rows for processing")

    # Gets a list of .pdf files for upload
    pdfs = [pdf_file for pdf_file in os.listdir(f"{input_dir}/{pdf_dir}")
            if '.pdf' in pdf_file]
    print(f"PDFs found:\n{pdfs}")

    # Exclude skip rows and empties
    rows = [r for r in rows
            if (r[pdf_field] not in skip_rows or r[pdf_field] != '')]

    # --------------------------------------
    if submit_metadata:
        for row in rows:
            global first_metadata_csv_write
            osti_submission_row = prep_row_for_transform(row, batch_group)
            with open(f"{run_output_dir}/{row[pdf_field]}.json", 'w') as f:
                json.dump(osti_submission_row, f, indent=3)

            osti_id = submit_metadata_to_osti(creds,
                                              row,
                                              osti_submission_row,
                                              run_output_dir)
            row['osti_id'] = osti_id
            print(f"OSTI Metadata submission successful: {osti_id}")

            # Output row data to file

            if first_metadata_csv_write:
                first_metadata_csv_write = False
                with open(f"{run_output_dir}/{output_filename_stem}_metadata.csv", 'w') as f:
                    fieldnames = list(row.keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerow(row)
            else:
                with open(f"{run_output_dir}/{output_filename_stem}_metadata.csv", 'a') as f:
                    fieldnames = list(row.keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row)

    # --------------------------------------
    if submit_media:
        # Only submit media where we have an OSTI ID
        rows = [r for r in rows if r.get('osti_id')]

        for row in rows:
            global first_media_csv_write
            # Standardization for submission dict
            if not row.get('Title'):
                row['Title'] = row['Publication Title']

            if f"{row[pdf_field]}.pdf" not in pdfs:
                print(f"[ERROR] {row[pdf_field]} not found in {input_dir}/{pdf_dir}. Continuing...")
                continue

            row['File URL'] = f"{input_dir}/{pdf_dir}/{row[pdf_field]}.pdf"
            row = submit_media_to_osti(creds,
                                       row,
                                       run_output_dir)

            # Output row data to file
            if first_media_csv_write:
                first_media_csv_write = False
                with open(f"{run_output_dir}/{output_filename_stem}_media.csv", 'w') as f:
                    fieldnames = list(row.keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerow(row)
            else:
                with open(f"{run_output_dir}/{output_filename_stem}_media.csv", 'a') as f:
                    fieldnames = list(row.keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row)


# ========================================
def submit_metadata_to_osti(creds, row, submission, run_output_dir):
    # nest for post metadata function below
    print(f"Submitting: {row['DOI']}")
    row['submission_json'] = submission

    response = elink_2.post_metadata(creds['osti_api'], row)
    elink_2.update_pub_with_response(row, response)

    with open(f"{run_output_dir}/{row[pdf_field]}_metadata_response.json", 'w') as f:
        json.dump(row['response_json'], f, indent=3)

    if not row['response_success']:
        print("ROW ERROR.")
        print(row['response_json'])
        return None
    else:
        print("Metadata Submission OK.")
        print(f"OSTI ID: {row['response_json']['osti_id']}")
        return row['response_json']['osti_id']


# ========================================
def submit_media_to_osti(creds, row, run_output_dir):
    media_response = elink_2.post_media(creds['osti_api'], row, local_file=True)
    row = elink_2.update_pub_with_media_response(row, media_response)

    if row['media_response_success']:
        print("Media submission OK.")
        with open(f"{run_output_dir}/{row[pdf_field]}_media_response.json", 'w') as f:
            json.dump(row['media_response_json'], f, indent=3)
    else:
        print(f"Media submission failure: {media_response.status_code}")

    return row


# stub for main
if __name__ == "__main__":
    main()
