import requests
from pprint import pprint


def submit_pubs(new_osti_pubs, osti_creds, submission_limit):

    new_osti_pubs_with_responses = []

    for i, osti_pub in enumerate(new_osti_pubs, 1):

        if i > submission_limit:
            print("Submission limit hit. Breaking submission loop.")
            break

        print(f"\nSubmitting: {i}, Publication ID: {osti_pub['id']}")

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
            print("Submission Failure:")
            print(response.json())
            osti_pub['response_success'] = False

        else:
            print("Submission OK.")
            osti_pub['response_success'] = True
            osti_pub['osti_id'] = osti_pub['response_json']['osti_id']

            # If metadata submission was successful, submit the PDF
            print(f" + Submitting media: {osti_pub['File URL']}")
            media_response = submit_media(osti_creds, osti_pub)
            osti_pub['media_response_code'] = media_response.status_code
            osti_pub['media_response_json'] = media_response.json()

            if media_response.status_code > 300:
                print(f"   Media submission failure: {media_response.status_code}")
                print(media_response.json())
                osti_pub['media_response_success'] = False
                osti_pub['media_file_id'] = None

            else:
                print("   Media submission OK.")
                osti_pub['media_response_success'] = True
                osti_pub['media_file_id'] = media_response.json()['files'][0]['media_file_id']

        new_osti_pubs_with_responses.append(osti_pub)

    return new_osti_pubs_with_responses


def submit_media(osti_creds, osti_pub):
    req_url = f"{osti_creds['base_url']}/media/{str(osti_pub['osti_id'])}"

    params = {'url': osti_pub['File URL'],
              'title': osti_pub['title']}

    # NOTE! Don't include a Content-Type header for media.
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}

    # Get the PDF file from url
    pdf_response = requests.get(osti_pub['File URL'])

    media_response = requests.post(req_url,
                                   headers=headers,
                                   params=params,
                                   files={'file': pdf_response.content})
    return media_response

    if media_response.status_code > 300:
        print(f"Media submission failure: {media_response.status_code}")
        print(media_response.json())
        return False

    else:
        print("Media submission OK.")
        return media_response.json()


def submit_metadata_updates(updated_osti_pubs, osti_creds, submission_limit):
    updated_osti_pubs_with_responses = []

    for i, osti_pub in enumerate(updated_osti_pubs, 1):

        if i > submission_limit:
            print("Submission limit hit. Breaking submission loop.")
            break

        print(f"\nSubmitting update: {i}, Elements Pub. ID: {osti_pub['id']}, OSTI ID: {osti_pub['osti_id']}")

        req_url = f"{osti_creds['base_url']}/records/{osti_pub['osti_id']}/submit"
        headers = {'Authorization': 'Bearer ' + osti_creds['token']}
        response = requests.put(
            req_url,
            json=osti_pub['submission_json'],
            headers=headers)

        # Save the response data
        osti_pub['response_status_code'] = response.status_code
        osti_pub['response_json'] = response.json()

        if response.status_code >= 300:
            print("Submission Failure:")
            print(response.json())
            osti_pub['response_success'] = False

        else:
            print("Submission OK.")
            osti_pub['response_success'] = True
            osti_pub['osti_id'] = osti_pub['response_json']['osti_id']

            # If metadata submission was successful, submit the PDF
            media_response = submit_media(osti_creds, osti_pub)
            osti_pub['media_response_code'] = media_response.status_code

            if media_response.status_code > 300:
                osti_pub['media_response_success'] = False
                osti_pub['media_file_id'] = None
            else:
                osti_pub['media_response_success'] = True
                osti_pub['media_response_json'] = media_response.json()
                osti_pub['media_file_id'] = media_response.json()['files'][0]['media_file_id']

        updated_osti_pubs_with_responses.append(osti_pub)

    return updated_osti_pubs_with_responses
