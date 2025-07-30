import program_setup
import elink_2_functions as elink_2
import requests
from pprint import pprint


# =======================================
def main2():
    args = program_setup.process_args()
    creds = program_setup.assign_creds(args)

    # Send the query, then convert and process the results
    response = general_api_query(creds['osti_api'])
    pubs = response.json()
    process_pubs(pubs)


def general_api_query(osti_creds):
    req_url = f"{osti_creds['base_url']}/records"
    headers = {'Authorization': 'Bearer ' + osti_creds['token']}
    params = {'site_ownership_code': 'LBNLSCH',
              'date_first_submitted_from': '06/01/2024'}

    response = requests.get(req_url, params=params, headers=headers)
    return response


def process_pubs(pubs):
    print(f"{len(pubs)} total publications found.")
    # for item in pubs:
    #     pprint(item)


# =======================================
# Stub for main
if __name__ == "__main__":
    main2()
