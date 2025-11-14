import osti_id_batch_writer_program_setup as setup
import requests
from pprint import pprint
from copy import deepcopy

test_mode = True
reset_mode = True


# =======================================
# Main
def main():
    creds = setup.get_creds(test_mode)
    mysql_conn = setup.get_cdl_connection(creds['cdl_db'])

    with mysql_conn.cursor() as cursor:
        print("Queueing new OSTI submissions...")
        enqueue_new_osti_submissions(cursor)

        print("Running updates loop...\n")
        run_updates(cursor, creds['eschol_api'])

    mysql_conn.close()


# =======================================
# Adds new OSTI submissions to the queue
def enqueue_new_osti_submissions(cursor):
    with open("osti_id_queue_updates.sql") as f:
        queue_update_query = f.read()
    cursor.execute(queue_update_query)
    print(f"{cursor.rowcount} new rows enqueued for osti_id updates.")


# =======================================
# Main update loop
def run_updates(cursor, eschol_api_creds):
    print("Querying osti_id queue for next row...")
    row = get_next_queue_row(cursor)
    row['eschol_id'] = 'qtttrmz60v'
    row['osti_id'] = 'test-999'

    print("Grabbing item values from eSchol API...")
    item_values = get_item_values(row, eschol_api_creds)
    old_item_values = deepcopy(item_values)

    print("Sending OSTI ID update to eSchol API...")
    send_local_id_updates(row, eschol_api_creds, item_values)

    print("Verifying existing local IDs were preserved...")
    updated_item_values = get_item_values(row, eschol_api_creds)
    verify_update(old_item_values, updated_item_values)

    if not test_mode:
        print("Updating queue row...")
        update_queue_row(cursor, row)


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
            print(f"{o}\t\t{n}")
            if o != n:
                raise 'OLD VALUES NOT FOUND IN NEW ESCHOL ITEM. Exiting.'
    if reset_mode:
        print("Running in reset mode -- Old and new values:")
        pprint(old_values)
        pprint(new_values)
    else:
        compare_values(old_values, new_values)
    exit()


# =======================================
def get_next_queue_row(cursor):
    queue_table = 'eschol_api_queue_test' if test_mode else 'eschol_api_queue'
    next_queue_row_query = f"select * from {queue_table} where updated=0 limit 1;"
    cursor.execute(next_queue_row_query)
    row = cursor.fetchone()
    return row


# =======================================
def get_item_values(row, creds):
    local_id_query = """
        query getLocalIds($input_id: ID!) {
            item(id:$input_id) {
                id
                localIDs {
                    id scheme subScheme }}}"""

    local_id_vars = {'input_id': f"ark:/13030/{row['eschol_id']}"}

    response = query_eschol_api(creds, local_id_query, local_id_vars)
    return response['data']['item']


# =======================================
def send_local_id_updates(row, creds, item_values):

    # Add the OSTI_ID to the LocalIDs array in item_values
    item_values['localIDs'].append({
        'id': f"{row['osti_id']}",
        'scheme': 'OTHER_ID',
        'subScheme': 'osti'})

    # This is for testing, it removes any localIDs with "osti" subschemes
    if reset_mode:
        item_values['localIDs'] = [
            i for i in item_values['localIDs']
            if 'osti' not in i['subScheme'].lower()]

    mutation_query = """
        mutation updateLocalIDs($input: UpdateLocalIDsInput!) { 
            updateLocalIDs(input: $input) { message } 
        }"""

    mutation_vars = {'input': item_values}
    query_eschol_api(creds, mutation_query, mutation_vars)


# =======================================
def update_queue_row(cursor, row):
    queue_table = 'eschol_api_queue_test' if test_mode else 'eschol_api_queue'
    update_queue_row_query = f"update {queue_table} set updated=1 where id={row['id']};"
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
    pprint(response.json())
    if response.status_code != 200:
        raise "Non-200 eSchol API response. Exiting"
    else:
        return response.json()


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
