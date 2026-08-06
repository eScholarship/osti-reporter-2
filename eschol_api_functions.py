import cdl_osti_db_functions as cdl
from time import sleep


# Updates the Local IDs in the eschol API.
def update_eschol_api(rows, eschol_api_creds, cdl_db_creds):

    for row in rows:

        # Skip unsucsefull E-Link API submissions
        if row.get('response_success') is not True:
            print("(Skipping a failed OSTI submission)")
            continue


        response = send_local_id_updates(row, eschol_api_creds)
        row['eschol_api_response_code'] = response.status_code
        if not 200 <= response.status_code <= 299:
            row['eschol_api_success'] = False
        else:
            row['eschol_api_success'] = True

        cdl.update_with_eschol_api(row, cdl_db_creds)
        sleep(10)

    return rows


def send_local_id_updates(row, creds):

    mutation_query = """
        mutation updateLocalIDs($input: UpdateLocalIDsInput!) { 
            updateLocalIDs(input: $input) { message } 
        }"""

    input_values = {
        # 'id': row['eschol_id'],
        'id': row['eSchol ID'],
        # 'published': row['pub_date'].strftime('%Y-%m-%d'),
        'published': row['pub_date_for_db'].strftime('%Y-%m-%d'),
        'localIDs': [{
            'id': f"{row['osti_id']}",
            'scheme': 'OTHER_ID',
            'subScheme': 'osti_id'}]}

    mutation_vars = {'input': input_values}
    response = query_eschol_api(creds, mutation_query, mutation_vars)
    return response


# =======================================
# Generic function for sending HTTP Reqs to eSchol GraphQL API
def query_eschol_api(creds, query, mutation_vars):
    import requests

    # Set headers cookies
    headers = {"PRIVILEGED": creds['priv-key'],
               "user-agent": 'cdl'}
    if creds.get('cookie'):
        cookies = dict(ACCESS_COOKIE=creds['cookie'])
    else:
        cookies = {}

    # Package the query and vars
    json = {"query": query, "variables": mutation_vars}

    # Send the req
    response = requests.post(
        url=creds['endpoint'],
        headers=headers,
        cookies=cookies,
        json=json)

    # Print response
    print(f"Response: {response.status_code} {response.reason}")
    return response
