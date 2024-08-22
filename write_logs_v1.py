def output_submissions(log_folder, new_osti_pubs):
    for index, osti_pub in enumerate(new_osti_pubs):
        filename = "V1-" + str(index) + "-SUBMISSION"
        with open(log_folder + "/" + filename + ".xml", "wb") as out_file:
            out_file.write(osti_pub['submission_xml_string'])


def output_responses(log_folder, new_osti_pubs):
    responses = [pub['response_xml_text'] for pub in new_osti_pubs]
    for index, response_xml_text in enumerate(responses):
        filename = "V1-" + str(index) + "-RESPONSE"
        with open(log_folder + "/" + filename + ".xml", "w") as out_file:
            out_file.write(response_xml_text)
