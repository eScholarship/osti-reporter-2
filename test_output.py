import csv
import json


# -----------------------------
def output_temp_table_query(sql):
    with open("test_output/temp_table_query.sql", "w") as outfile:
        outfile.write(sql)


# -----------------------------
def output_temp_table_results(rows):
    with open("test_output/temp_table_results.csv", "w") as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(rows[0].keys())
        for row in rows:
            csv_writer.writerow(row.values())


# -----------------------------
def output_elements_query_results(new_osti_pubs):

    with open('test_output/elements-query-result.csv', 'w') as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(new_osti_pubs[0].keys())
        for row in new_osti_pubs:
            csv_writer.writerow(row.values())


# -----------------------------
def output_submissions(new_osti_pubs, elink_version):

    if elink_version == 1:
        for index, osti_pub in enumerate(new_osti_pubs):
            filename = "v1-test-" + str(index)
            with open("test_output/v1_submissions/" + filename + ".xml", "wb") as out_file:
                out_file.write(osti_pub['submission_xml_string'])

    elif elink_version == 2:
        for index, osti_pub in enumerate(new_osti_pubs):
            filename = "v2-test-" + str(index)
            osti_pub_json_string = json.dumps(osti_pub['submission_json'], indent=4)
            with open("test_output/v2_submissions/" + filename + ".json", "w") as out_file:
                out_file.write(osti_pub_json_string)
