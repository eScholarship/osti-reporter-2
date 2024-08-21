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
    with open(log_folder + "/temp_table_query.sql", "w") as outfile:
        outfile.write(sql)


def output_temp_table_results(log_folder, rows):
    with open(log_folder + "/temp_table_results.csv", "w") as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(rows[0].keys())
        for row in rows:
            row = {k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()}
            csv_writer.writerow(row.values())


def output_elements_query_results(log_folder, new_osti_pubs):
    with open(log_folder + '/elements_query_result.csv', 'w') as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(new_osti_pubs[0].keys())
        for row in new_osti_pubs:
            csv_writer.writerow(row.values())


def output_submissions(log_folder, new_osti_pubs, elink_version, submission_type="NEW"):

    if elink_version == 1:
        for index, osti_pub in enumerate(new_osti_pubs):
            filename = "V1-" + str(index) + "-SUBMISSION"
            with open(log_folder + "/" + filename + ".xml", "wb") as out_file:
                out_file.write(osti_pub['submission_xml_string'])

    elif elink_version == 2:
        for index, osti_pub in enumerate(new_osti_pubs):
            filename = f"V2-{submission_type}-{str(index)}-SUBMISSION"
            osti_pub_json_string = json.dumps(osti_pub['submission_json'], indent=4)
            with open(log_folder + "/" + filename + ".json", "w") as out_file:
                out_file.write(osti_pub_json_string)


def output_responses(log_folder, new_osti_pubs, elink_version):

    if elink_version == 1:
        responses = [pub['response_xml_text'] for pub in new_osti_pubs]
        for index, response_xml_text in enumerate(responses):
            filename = "V1-" + str(index) + "-RESPONSE"
            with open(log_folder + "/" + filename + ".xml", "w") as out_file:
                out_file.write(response_xml_text)

    elif elink_version == 2:
        responses = [pub['response_json'] for pub in new_osti_pubs]
        for index, response_json in enumerate(responses):
            filename = "V2-" + str(index) + "-RESPONSE"
            response_json_string = json.dumps(response_json, indent=4)
            with open(log_folder + "/" + filename + ".json", "w") as out_file:
                out_file.write(response_json_string)


def output_json_generic(log_folder, data, elink_version, filename):

    with open(f"{log_folder}/V{elink_version}-{filename}.json", "w") as out_file:
        out_file.write(json.dumps(data, indent=4, default=serialize_datetime))


def console_final_report(new_submissions, metadata_updates, pdf_updates):
    pass

    # TK TK pick up here
    print('\n\n\n----------------')
    print('FINAL REPORT:\n')

    if not new_submissions:
        print("New submissions: None.")
    else:
        print(f"New submissions: {len(new_submissions)}")
        success_pubs = [pub for pub in new_submissions if pub['response_success'] is True]
        fail_pubs = [pub for pub in new_submissions if pub['response_success'] is False]
        print(f"Success: {len(success_pubs)}")
        print(f"Failure: {len(fail_pubs)}")
        print("Failures itemized:")

        for pub in fail_pubs:
            pass

    pass
