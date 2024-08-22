import requests


# New Metadata submissions
def submit_new_pubs(pubs_for_metadata_submission, osti_creds):
    for pub in pubs_for_metadata_submission:
        print(f"\nSubmitting Publication ID: {pub['id']}")

        response = post_metadata(osti_creds, pub)
        pub['response_status_code'] = response.status_code
        pub['response_json'] = response.json()
        pub['response_success'] = (response.status_code < 300)

        if pub['response_success']:
            print("Submission OK.")
            pub['osti_id'] = pub['response_json']['osti_id']
        else:
            print("Submission Failure:")
            print(pub['response_json'])

    return pubs_for_metadata_submission


# New PDF submissions metadata without PDFs.
def submit_new_pdfs(pubs_for_media_submission, osti_creds):
    for pub in pubs_for_media_submission:
        print(f"\nSubmitting media: Elements ID {pub['id']}, OSTI ID {pub['osti_id']}\n"
              f"PDF: {pub['File URL']}")

        media_response = post_media(osti_creds, pub)
        pub['media_response_code'] = media_response.status_code
        pub['media_response_json'] = media_response.json()
        pub['media_response_success'] = (media_response.status_code < 300)

        if pub['media_response_success']:
            print("Media submission OK.")
            pub['media_file_id'] = media_response.json()['files'][0]['media_file_id']
        else:
            print(f"Media submission failure: {media_response.status_code}")
            print(pub['media_response_json'])
            pub['media_file_id'] = None

    return pubs_for_media_submission


# Update existing OSTI metadata
def submit_metadata_updates(updated_osti_pubs, osti_creds):
    for pub in updated_osti_pubs:
        print(f"\nSubmitting update: Elements Pub. ID: {pub['id']}, OSTI ID: {pub['osti_id']}")

        response = put_metadata(osti_creds, pub)
        pub['response_status_code'] = response.status_code
        pub['response_json'] = response.json()
        pub['response_success'] = (response.status_code < 300)

        if pub['response_success']:
            print("Metadata Update Submission OK.")
        else:
            print("Metadata Update Submission Failure:")
            print(response.json())

    return updated_osti_pubs


# Replace PDF, or try a new PDF if the eSchol OSTI DB contains an error response.
def submit_media_updates(updated_media_pubs, osti_creds, submission_limit):
    # TK Assess whether the initial submission failed (ie, no media_file_id), or it's a replaced PDF.
    pass


def post_metadata(osti_creds, pub):
    # Build the request & send it to OSTI
    req_url = f"{osti_creds['base_url']}/records/submit"
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}
    response = requests.post(req_url, json=pub['submission_json'], headers=headers)
    return response


def put_metadata(osti_creds, pub):
    req_url = f"{osti_creds['base_url']}/records/{pub['osti_id']}/submit"
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}
    response = requests.put(req_url, json=pub['submission_json'], headers=headers)
    return response


def post_media(osti_creds, pub):
    req_url = f"{osti_creds['base_url']}/media/{pub['osti_id']}"
    params = {'url': pub['File URL'], 'title': pub['title']}
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}

    # Get the PDF file data from url
    pdf_response = requests.get(pub['File URL'])

    # Send the post with the PDF data
    media_response = requests.post(req_url, headers=headers, params=params, files={'file': pdf_response.content})
    return media_response


def put_media(osti_creds, pub):
    req_url = f"{osti_creds['base_url']}/media/{pub['osti_id']}/{pub['media_file_id']}"
    params = {'url': pub['File URL'], 'title': pub['title']}
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}

    # Get the PDF file data from url
    pdf_response = requests.get(pub['File URL'])

    # Send the post with the PDF data
    media_response = requests.put(req_url, headers=headers, params=params, files={'file': pdf_response.content})
    return media_response

