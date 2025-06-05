import csv
import json
import os
from datetime import datetime


# Helper function for outputting datetime in JSON
def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def create_log_folder():
    log_folder = "logs/" + datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
    os.mkdir(log_folder)
    return log_folder


def output_temp_table_query(log_folder, sql):
    with open(f"{log_folder}/temp_table_query.sql", "w") as outfile:
        outfile.write(sql)


def output_temp_table_results(log_folder, rows):
    with open(f"{log_folder}/temp_table_results.csv", "w") as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(rows[0].keys())
        for row in rows:
            row = {k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()}
            csv_writer.writerow(row.values())


def output_elements_query_results(log_folder, new_osti_pubs):
    with open(f"{log_folder}/elements_query_result.csv", 'w') as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(new_osti_pubs[0].keys())
        for row in new_osti_pubs:
            csv_writer.writerow(row.values())


def output_submissions(log_folder, new_osti_pubs, submission_type="NEW"):
    for index, osti_pub in enumerate(new_osti_pubs):
        filename = f"{submission_type}-{str(index)}-SUBMISSION"
        osti_pub_json_string = json.dumps(osti_pub['submission_json'], indent=4)
        with open(f"{log_folder}/{filename}.json", "w") as out_file:
            out_file.write(osti_pub_json_string)


def output_responses(log_folder, new_osti_pubs):
    responses = [pub['response_json'] for pub in new_osti_pubs]
    for index, response_json in enumerate(responses):
        filename = f"{str(index)}-RESPONSE"
        response_json_string = json.dumps(response_json, indent=4)
        with open(f"{log_folder}/{filename}.json", "w") as out_file:
            out_file.write(response_json_string)


def output_json_generic(log_folder, data, filename):
    with open(f"{log_folder}/{filename}.json", "w") as out_file:
        out_file.write(json.dumps(data, indent=4, default=serialize_datetime))


def print_final_report(new_osti_pubs, new_osti_pdfs, osti_metadata_updates, osti_pdf_updates):

    report_header = "\n ================ OSTI REPORTER: SUMMARY"

    def report_builder(pubs, message, success_field, failure_json_field):
        print("\n--------------------")

        if not pubs:
            print(f"No {message}")

        else:
            success = [p for p in pubs if p[success_field]]
            failure = [p for p in pubs if not p[success_field]]

            print(f"{len(pubs)} total {message}")
            print(f"{len(success)} successes, {len(failure)} failures.")

            if success:
                print(f"\n{len(success)} successful submission(s):")
                for s in success:
                    print(s['id'])

            if failure:
                print(f"\n{len(failure)} failed submission(s):")
                for f in failure:
                    print(f"\n{f['id']}\n{f[failure_json_field]}")

    print(report_header)

    report_builder(new_osti_pubs, "new pubs submitted to OSTI.",
                   'response_success', 'response_json')

    report_builder(new_osti_pdfs, "PDFs submitted for newly-added publications.",
                   'media_response_success', 'media_response_json')

    report_builder(osti_metadata_updates, "pubs with updated metadata sent to OSTI.",
                   'response_success', 'response_json')

    report_builder(osti_pdf_updates, "replacement PDFs sent to OSTI.",
                   'media_response_success', 'media_response_json')

    print(report_header)
