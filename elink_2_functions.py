# OSTI E-Link 2 documentation https://review.osti.gov/elink2api/
import requests
import mimetypes
from time import sleep
from requests_toolbelt.multipart.encoder import MultipartEncoder
import cdl_osti_db_functions as cdl


# New Metadata submissions
def submit_new_pubs(pubs_for_metadata_submission, osti_creds, mysql_creds):
    submission_counter = 0
    for pub in pubs_for_metadata_submission:
        submission_counter += 1
        print(f"\nSubmission {submission_counter}/{len(pubs_for_metadata_submission)}")
        print(f"Submitting Publication ID: {pub['id']}")

        try:
            response = post_metadata(osti_creds, pub)
            pub = update_pub_with_response(pub, response)

            if pub['response_success']:
                print("Metadata Submission OK.")
                pub['osti_id'] = pub['response_json']['osti_id']

                print("Updating CDL DB with response data.")
                cdl.insert_new_metadata_submission(pub, mysql_creds)
                sleep(3)

                print(f"Submitting media: Elements ID {pub['id']}, "
                      f"OSTI ID {pub['osti_id']}, PDF: {pub['File URL']}")

                media_response = post_media(osti_creds, pub)
                pub = update_pub_with_media_response(pub, media_response)

                if pub['media_response_success']:
                    print("Media submission OK.")
                else:
                    print(f"Media submission failure: {media_response.status_code}")

                print("Updating CDL DB with Media data (will include media failure codes).")
                cdl.update_media_submission(pub, mysql_creds)

            else:
                print("Submission Failure:")
                print(pub['response_json'])

        except Exception as e:
            print(e)
            print()
            raise f"Failed while submitting a new record: Elements ID {pub['id']}"

    return pubs_for_metadata_submission


# Update existing OSTI metadata
def submit_metadata_updates(updated_osti_pubs, osti_creds, mysql_creds):
    for pub in updated_osti_pubs:
        print(f"\nSubmitting update: Elements Pub. ID: {pub['id']}, OSTI ID: {pub['osti_id']}")

        try:
            response = put_metadata(osti_creds, pub)
            pub = update_pub_with_response(pub, response)

            if pub['response_success']:
                print("Metadata Update Submission OK.")
                print("Updating CDL DB with response data.")
                cdl.update_osti_db_metadata(pub, mysql_creds)
            else:
                print("Metadata Update Submission Failure:")
                print(response.json())

        except Exception as e:
            print(e)
            print()
            raise f"Failed while submitting a metadata update: Elements ID {pub['id']}"

    return updated_osti_pubs


# Replace PDF, or try a new PDF if the eSchol OSTI DB contains an error response.
def submit_media_updates(updated_media_pubs, osti_creds, mysql_creds):
    for pub in updated_media_pubs:
        print(f"\nSubmitting media update: Elements ID {pub['id']}, OSTI ID {pub['osti_id']},"
              f"\nMedia ID: {pub['media_id']}, Media File ID: {pub['media_file_id']}"
              f"\nPDF: {pub['File URL']}")

        try:
            # A null media_id means there was an error with the first pdf submission,
            # so it requires a post() b/c no media file currently exists.
            media_response = None
            if pub['media_id'] is None or 'media_id' not in pub.keys():
                print("No media file ID: New PDF submission.")
                media_response = post_media(osti_creds, pub)
            else:
                print("Existing file ID: Updating PDF.")
                media_response = put_media(osti_creds, pub)

            pub = update_pub_with_media_response(pub, media_response)

            if pub['media_response_success']:
                print("Media update OK.")
            else:
                print(f"Media update failure: {media_response.status_code}")

            if pub['media_response_code'] == 404:
                print("Updating CDL DB to indicate a deleted Media ID.")
                cdl.update_media_deleted_id(pub, mysql_creds)
            else:
                print("Updating CDL DB with Media data (includes non-404 failure codes).")
                cdl.update_media_submission(pub, mysql_creds)

        except Exception as e:
            print(e)
            print()
            raise f"Failed while updating a PDF: Elements ID {pub['id']}"

    return updated_media_pubs


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

    # Get the PDF file data from url
    pdf_filename = pub['File URL'].split('/')[-1]
    pdf_response = requests.get(pub['File URL'], stream=True)
    pdf_response.raw.decode_content = True

    mp_encoder = MultipartEncoder(
        fields={'file': (pdf_filename, pdf_response.content, 'application/pdf')}
    )

    headers = {'Authorization': 'Bearer ' + osti_creds['token'],
               'Content-Type': mp_encoder.content_type}
    params = {'title': pub['title']}

    # Send the post with the PDF data
    media_response = requests.post(
        req_url, headers=headers, params=params, data=mp_encoder)

    return media_response


def put_media(osti_creds, pub):
    req_url = f"{osti_creds['base_url']}/media/{pub['osti_id']}/{pub['media_id']}"

    # Get the PDF file data from url
    pdf_filename = pub['File URL'].split('/')[-1]
    pdf_response = requests.get(pub['File URL'], stream=True)
    pdf_response.raw.decode_content = True

    mp_encoder = MultipartEncoder(
        fields={'file': (pdf_filename, pdf_response.content, 'application/pdf')})

    headers = {'Authorization': 'Bearer ' + osti_creds['token'],
               'Content-Type': mp_encoder.content_type}
    params = {'title': pub['title']}

    # Send the post with the PDF data
    media_response = requests.put(
        req_url, headers=headers, params=params, data=mp_encoder)

    return media_response


# Adds metadata response data to publication dict
def update_pub_with_response(pub, metadata_response):
    pub['response_status_code'] = metadata_response.status_code
    pub['response_json'] = metadata_response.json()
    pub['response_success'] = (metadata_response.status_code < 300)
    return pub


# Adds media response data to publication dict
def update_pub_with_media_response(pub, media_response):

    # The Media API can return non-JSON data, which will trigger a decode error.
    try:
        pub['media_response_code'] = media_response.status_code
        pub['media_response_json'] = media_response.json()
        pub['media_response_success'] = (media_response.status_code < 300)

    except Exception as e:
        print("Nonstandard media API response.\n"
              "Saving as submission failure.\nResponse text:")
        print(media_response)
        pub['media_response_code'] = None
        pub['media_response_json'] = None
        pub['media_response_success'] = False

    if pub['media_response_success']:
        pub['media_id'] = media_response.json()['files'][0]['media_id']
        pub['media_file_id'] = media_response.json()['files'][0]['media_file_id']
    else:
        pub['media_id'] = None
        pub['media_file_id'] = None

    return pub

def get_pubs_by_workflow_status(osti_creds, workflow_status):
    # Build the request & send it to OSTI
    req_url = f"{osti_creds['base_url']}/records"
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}
    params = {
        'site_ownership_code': 'LBNLSCH',
        'date_first_submitted_from': '06/01/2025',
        'workflow_status': workflow_status}

    response = requests.get(req_url, params=params, headers=headers)
    return response


def get_hidden_pubs(osti_creds):
    # Build the request & send it to OSTI
    req_url = f"{osti_creds['base_url']}/records"
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}
    params = {
        'site_ownership_code': 'LBNLSCH',
        'date_first_submitted_from': '06/01/2025',
        'hidden_flag': 'true'}

    response = requests.get(req_url, params=params, headers=headers)
    return response
