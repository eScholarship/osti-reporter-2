import csv
import json


def create_log_folder():
    from datetime import datetime
    log_folder = "logs/" + datetime.today().strftime('%Y-%m-%d-%H-%M-%S')

    import os
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
            csv_writer.writerow(row.values())


def output_elements_query_results(log_folder, new_osti_pubs):
    with open(log_folder + '/elements_query_result.csv', 'w') as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(new_osti_pubs[0].keys())
        for row in new_osti_pubs:
            csv_writer.writerow(row.values())


def output_submissions(log_folder, new_osti_pubs, elink_version):

    if elink_version == 1:
        for index, osti_pub in enumerate(new_osti_pubs):
            filename = "V1-" + str(index)
            with open(log_folder + "/" + filename + ".xml", "wb") as out_file:
                out_file.write(osti_pub['submission_xml_string'])

    elif elink_version == 2:
        for index, osti_pub in enumerate(new_osti_pubs):
            filename = "V2-" + str(index)
            osti_pub_json_string = json.dumps(osti_pub['submission_json'], indent=4)
            with open(log_folder + "/" + filename + ".json", "w") as out_file:
                out_file.write(osti_pub_json_string)
