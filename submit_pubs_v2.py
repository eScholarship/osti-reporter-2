import requests
from pprint import pprint

def submit_pubs(new_osti_pubs, osti_creds, submission_limit):

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

        else:
            print("Submission OK.\n")
            osti_pub['response_success'] = True
            osti_pub['osti_id'] = osti_pub['response_json']['osti_id']

            # If metadata submission was successful, proceed with the media submission (ie: pdf)
            # media_response = submit_media(osti_creds, osti_pub)
            # osti_pub['media_response_status_code'] = media_response.status_code
            # osti_pub['media_response_json'] = media_response.json()

        new_osti_pubs_with_responses.append(osti_pub)

    return new_osti_pubs_with_responses


def submit_media(osti_creds, osti_pub):

    media_submission_json = {
        'id': osti_pub['osti_id'],
        'title': osti_pub['title'],
        'url': osti_pub['File URL']}

    print(f"File submission: {osti_pub['File URL']}")
    params = {'url': osti_pub['File URL']}

    file_response = requests.get(osti_pub['File URL'])
    # with open(file_response.content, 'rb') as pdf_file:
    #    pdf_file_data = pdf_file.read()

    print("Debug time:::::")
    # print(type(file_response.raw))
    print(type(file_response.content))

    pdf_file_data = file_response.content
    pdf_file_data = ''.join(format(byte, '08b') for byte in pdf_file_data)
    print(type(pdf_file_data))

    req_url = osti_creds['base_url'] + "/media/" + str(osti_pub['osti_id'])
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}
    media_response = requests.post(
        req_url,
        params=params,
        data=pdf_file_data,
        headers=headers)

    print("Media response:")
    pprint(media_response.status_code)
    mrj = media_response.json()
    pprint(mrj)

    # stream = True
    exit()

    return media_response
