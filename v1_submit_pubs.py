def submit_pubs(new_osti_pubs, osti_creds, submission_limit):

    import requests
    import xml.etree.ElementTree as ET

    new_osti_pubs_with_responses = []

    for i, osti_pub in enumerate(new_osti_pubs, 1):

        if i > submission_limit:
            print("\nSubmission limit hit. Breaking submission loop.")
            break

        print("\nSubmission:", i, "\nPublication ID: ", osti_pub['id'])

        # Build the request
        req_url = osti_creds['base_url']
        headers = {'Content-type': 'text/xml'}
        auth_data = (osti_creds['username'], osti_creds['password'])

        response = requests.post(
            req_url,
            data=osti_pub['submission_xml_string'],
            headers=headers,
            auth=auth_data)

        # Save the response data
        osti_pub['response_status_code'] = response.status_code
        osti_pub['response_xml_text'] = response.text

        if response.status_code >= 300:
            print("Failure response code:", response.status_code)
            print(response.text)
            osti_pub['response_success'] = False

        else:
            print("Submission OK.")
            osti_pub['response_success'] = True

            # If success, get the OSTI ID from the response text
            root = ET.fromstring(response.text)
            osti_pub['osti_id'] = root.find('record').find('osti_id').text
            print("OSTI ID:", osti_pub['osti_id'])

        new_osti_pubs_with_responses.append(osti_pub)

    return new_osti_pubs_with_responses
