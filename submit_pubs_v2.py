def submit_pubs(new_osti_pubs, osti_creds, submission_limit):
    import requests
    from pprint import pprint

    new_osti_pubs_with_responses = []

    for i, osti_pub in enumerate(new_osti_pubs, 1):

        if i > submission_limit:
            print("Submission limit hit. Breaking submission loop.")
            break

        print("Submitting:", i, ": Publication ID: ", osti_pub['id'])

        req_url = osti_creds['base_url'] + "/records/submit"
        headers = {'Authorization': 'Bearer ' + osti_creds['token']}
        response = requests.post(
            req_url,
            json=osti_pub['submission_json'],
            headers=headers)

        # Save the response data
        osti_pub['response_status_code'] = response.status_code
        osti_pub['response_json'] = response.json()

        if response.status_code >= 300:
            print("Response status code > 300...\n")
            osti_pub['response_success'] = False

            # for Testing
            pprint(osti_pub['submission_json'])
            print(response)
            pprint(response.json())

        else:
            print("Submission OK.\n")
            osti_pub['response_success'] = True
            osti_pub['osti_id'] = osti_pub['response_json']['osti_id']

        new_osti_pubs_with_responses.append(osti_pub)

    return new_osti_pubs_with_responses
