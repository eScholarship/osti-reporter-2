import requests
from pprint import pprint
import osti_id_batch_writer_program_setup as setup

test_mode = True


# =======================================
# Main
def main():
    creds = setup.get_creds()
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
    with open("osti_id_queue_update.sql") as f:
        queue_update_query = f.read()
    cursor.execute(queue_update_query)
    print(f"{cursor.rowcount} new rows enqueued for osti_id updates.")


# =======================================
# Main update loop
def run_updates(cursor, eschol_api_creds):
    print("Querying osti_id queue for next row...")
    row = get_next_queue_row(cursor)

    print("Grabbing local IDs from eSchol API...")
    local_ids = get_local_ids(row, eschol_api_creds)

    print("Updating eSchol API...")
    update_eschol_api(row, eschol_api_creds)

    if not test_mode:
        print("Updating queue row...")
        update_queue_row(cursor, row)


# =======================================
def get_next_queue_row(cursor):
    queue_table = 'eschol_api_queue_test' if test_mode else 'eschol_api_queue'
    next_queue_row_query = f"select * from {queue_table} where updated=0 limit 1;"
    cursor.execute(next_queue_row_query)
    row = cursor.fetchone()
    return row


# =======================================
def get_local_ids(row, creds):
    local_id_query = """
        query getLocalIds($input_id: ID!) {
            item(id:$input_id) {
                id
                localIDs {
                    id scheme subScheme }}}"""

    local_id_vars = {'input_id': f"ark:/13030/{row['eschol_id']}"}
    response = query_eschol_api(creds, local_id_query, local_id_vars)
    print()
    pprint(type(response))
    pprint(response)
    exit()


# =======================================
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
    print(f"Response: {response.status_code} -- {response.reason}")
    print(response.text)
    if response.status_code != 200:
        raise "Non-200 eSchol API response. Exiting"
    else:
        return response.json()


# =======================================
def update_queue_row(cursor, row):
    queue_table = 'eschol_api_queue_test' if test_mode else 'eschol_api_queue'
    update_queue_row_query = f"update {queue_table} set updated=1 where id={row['id']};"
    cursor.execute(update_queue_row_query)
    print(f"{cursor.rowcount} row updated.")


# =======================================
def update_eschol_api(row, creds):
    test_query = 'query getItem($input_id: ID!){ item(id:$input_id) { id, title, rights } }'
    test_vars = {'input_id': f"ark:/13030/{row['eschol_id']}"}
    row['eschol_id'] = 'qtttrmz60v'
    print(f"escholID: {row['eschol_id']}")

    mutation_query = """
        mutation updateLocalIDs($input: UpdateLocalIDsInput!) { 
            updateLocalIDs(input: $input) { message } 
        }"""

    mutation_vars = {
        'input': {
            'id': 'qtttrmz60v',
            'localIDs': [
                {
                    'id': "3000968",
                    'scheme': "OA_PUB_ID"
                },
                {
                    'id': "test_value_4",
                    'scheme': "OTHER_ID",
                    'subScheme': "osti_id"
                }
            ]
        }}



#    json = {"query": test_query, "variables": test_vars} if test_mode else \
#       {"query": mutation_query, "variables": mutation_vars}

    json = {"query": mutation_query, "variables": mutation_vars}




# =======================================
# Stub for main
if __name__ == "__main__":
    main()
