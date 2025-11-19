import boto3
import pymysql
import requests
from pprint import pprint
from datetime import datetime
from time import sleep

# Global vars
test_mode = True
verbose_mode = True
reset_mode = False
error_reset = 3
error_count = error_reset
total_updates = 5


# =======================================
# Main
def main():
    creds = get_creds()
    mysql_conn = get_cdl_connection(creds['cdl_db'])
    osti_table = creds['cdl_db']['osti-table-test'] \
        if test_mode else creds['cdl_db']['osti-table']

    print("Running updates loop.\n")
    with mysql_conn.cursor() as cursor:
        for i in range(total_updates):
            run_single_update(creds, cursor, osti_table)

    mysql_conn.close()


# =======================================
# Update a single eSchol item with OSTI ID
def run_single_update(creds, cursor, osti_table):
    update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    global error_count

    print("Querying osti submissions for the next row to update.")
    row = get_next_queue_row(cursor, osti_table)
    sleep(10)

    print("Grabbing item values from eSchol API.")
    old_item_values = get_item_values(row, creds['eschol_api'])
    sleep(10)

    print("Sending OSTI ID update to eSchol API.")
    mutation_response = send_local_id_updates(row, creds['eschol_api'])
    sleep(10)

    if not 200 <= mutation_response.status_code <= 299:
        print(f"Failure response from eSchol API: {mutation_response.status_code}")
        if verbose_mode:
            pprint(mutation_response.text)
        update_submission_row_fail(cursor, osti_table, row, update_time, mutation_response)

        # If several errors occur in a row, exit.
        error_count = error_count - 1
        if error_count == 0:
            raise RuntimeError("SEQUENTIAL ERROR LIMIT REACHED. EXITING")

    else:
        print("Verifying existing local IDs were preserved.")
        updated_item_values = get_item_values(row, creds['eschol_api'])
        verify_update(old_item_values, updated_item_values)

        print("Updating osti submission row.")
        update_submission_row_success(cursor, osti_table, row, update_time, mutation_response)

        # Reset the error count after successful updates
        error_count = error_reset

    sleep(10)


# =======================================
def get_next_queue_row(cursor, osti_table):
    next_queue_row_query = f"""
        select id, eschol_id, osti_id
        from {osti_table} 
        where eschol_api_updated is null
        order by id asc limit 1"""
    cursor.execute(next_queue_row_query)
    row = cursor.fetchone()
    if verbose_mode:
        pprint(row)
    return row


# =======================================
def get_item_values(row, creds):
    local_id_query = """
        query getLocalIds($input_id: ID!) {
            item(id:$input_id) {
                id
                localIDs {
                    id 
                    scheme
                    subScheme}}}"""

    local_id_vars = {'input_id': f"ark:/13030/{row['eschol_id']}"}

    response = query_eschol_api(creds, local_id_query, local_id_vars)
    if verbose_mode:
        pprint(response)
        pprint(response.json())

    if not 200 <= response.status_code <= 299:
        pprint(response.text)
        raise RuntimeError("Non-2xx eSchol API mutation response. Exiting")

    return response.json()['data']['item']


# =======================================
def send_local_id_updates(row, creds):

    mutation_query = """
        mutation updateLocalIDs($input: UpdateLocalIDsInput!) { 
            updateLocalIDs(input: $input) { message } 
        }"""

    input_values = {
        'id': row['eschol_id'],
        'localIDs': [{
            'id': f"{row['osti_id']}",
            'scheme': 'OTHER_ID',
            'subScheme': 'osti_id'}]}

    mutation_vars = {'input': input_values}
    response = query_eschol_api(creds, mutation_query, mutation_vars)
    return response


# =======================================
def verify_update(old_values, new_values):
    def compare_values(o, n):
        if type(o) is dict:
            for key in o.keys():
                compare_values(o.get(key), n.get(key))
        elif type(o) is list:
            for i in range(len(o)):
                compare_values(o[i], n[i])
        else:
            if verbose_mode:
                print(f"{o}\t\t{n}")
            if o != n:
                raise ValueError('OLD VALUES NOT FOUND IN NEW ESCHOL ITEM. Exiting.')

    if reset_mode:
        print("Running in reset mode -- Old and new values:")
        pprint(old_values)
        pprint(new_values)
    else:
        compare_values(old_values, new_values)


# =======================================
def update_submission_row_success(cursor, osti_table, row, update_time, response):
    update_queue_row_query = f"""
        update {osti_table} set
        eschol_api_updated='{update_time}',
        eschol_api_response_code={response.status_code}
        where id={row['id']};"""
    cursor.execute(update_queue_row_query)
    print(f"{cursor.rowcount} row updated.")


# =======================================
def update_submission_row_fail(cursor, osti_table, row, update_time, response):
    update_queue_row_query = f"""
        update {osti_table} set
        eschol_api_updated='{update_time}',
        eschol_api_response_code={response.status_code},
        eschol_api_failure_reason='{response.text}'
        where id={row['id']};"""
    cursor.execute(update_queue_row_query)
    print(f"{cursor.rowcount} row updated.")


# =======================================
# Generic function for sending HTTP Reqs to eSchol GraphQL API
def query_eschol_api(creds, query, vars):
    # Set headers cookies
    headers = dict(PRIVILEGED=creds['priv-key'])
    cookies = dict(ACCESS_COOKIE=creds['cookie']) if test_mode else {}

    # Package the query and vars
    json = {"query": query, "variables": vars}

    # Send the req
    response = requests.post(
        url=creds['endpoint'],
        headers=headers,
        cookies=cookies,
        json=json)

    # Print response
    print(f"Response: {response.status_code} {response.reason}")
    return response


# =======================================
# Setup function, connects to AWS for creds
def get_creds():
    print("Connecting to AWS.")
    session = boto3.Session()

    def get_ssm_parameters(folder, names):
        ssm_client = session.client(service_name='ssm', region_name='us-west-2')
        param_names = [f"{folder}/{name}" for name in names]
        response = ssm_client.get_parameters(Names=param_names, WithDecryption=True)

        param_values = {
            (param['Name'].split('/')[-1]): param['Value']
            for param in response['Parameters']}

        return param_values

    creds = dict()

    creds['cdl_db'] = get_ssm_parameters(
        f"/pub-oapi-tools/tools-rds/prod",
        ['user', 'password', 'server', 'port', 'osti-db', 'driver',
         'osti-table', 'osti-table-test'])

    if test_mode:
        creds['eschol_api'] = get_ssm_parameters(
            f"/pub-oapi-tools/eschol-api/qa",
            ['endpoint', 'priv-key', 'cookie'])
    else:
        creds['eschol_api'] = get_ssm_parameters(
            f"/pub-oapi-tools/eschol-api/prod",
            ['endpoint', 'priv-key', 'cookie'])

    return creds


# =======================================
# connect to the mySql db
def get_cdl_connection(mysql_creds):
    try:
        mysql_conn = pymysql.connect(
            host=mysql_creds['server'],
            user=mysql_creds['user'],
            password=mysql_creds['password'],
            database=mysql_creds['osti-db'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True)

        return mysql_conn
    except Exception as e:
        print("ERROR WHILE CONNECTING TO MYSQL DATABASE.")
        raise e


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
