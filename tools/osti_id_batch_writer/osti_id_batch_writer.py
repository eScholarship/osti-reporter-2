import osti_id_batch_writer_program_setup as setup
import requests
from pprint import pprint
import datetime
from copy import deepcopy

test_mode = True
verbose_mode = True
reset_mode = False


# =======================================
# Main
def main():
    creds = setup.get_creds(test_mode)
    mysql_conn = setup.get_cdl_connection(creds['cdl_db'])

    with mysql_conn.cursor() as cursor:
        # print("Enqueueing new OSTI submissions.")
        # enqueue_new_osti_submissions(cursor)

        print("Running updates loop.\n")
        run_updates(cursor, creds['eschol_api'])

    mysql_conn.close()


# =======================================
# Adds new OSTI submissions to the queue
def enqueue_new_osti_submissions(cursor):
    enqueue_query = """
        INSERT INTO eschol_api_queue
        SELECT
            id, osti_id, elements_id, eschol_id,
            0 as `updated`
        FROM osti_submissions_live
        WHERE
            media_response_code between 200 and 299
            and osti_id not in (
                select eaq.osti_id
                from eschol_api_queue eaq);"""

    cursor.execute(enqueue_query)
    print(f"{cursor.rowcount} new rows enqueued for osti_id updates.")


# =======================================
# Main update loop
def run_updates(cursor, eschol_api_creds):
    # loop
    print("Querying osti submissions for the next row to update.")
    row = get_next_queue_row(cursor)

    print("Grabbing item values from eSchol API.")
    old_item_values = get_item_values(row, eschol_api_creds)

    # print("Saving old values for later verification.")
    # old_item_values = deepcopy(item_values)

    print("Sending OSTI ID update to eSchol API.")
    mutation_response = send_local_id_updates(row, eschol_api_creds)

    if not 199 < mutation_response.status_code < 300:
        print(f"Failure response from eSchol API: {mutation_response.status_code}")
        pprint(mutation_response.text)
        update_submission_row_fail(cursor, row, mutation_response)
        exit()

    else:
        # time.sleep(30)
        print("Verifying existing local IDs were preserved.")
        updated_item_values = get_item_values(row, eschol_api_creds)
        verify_update(old_item_values, updated_item_values)

        print("Updating osti submission row.")
        update_submission_row_success(cursor, row, mutation_response)


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
def get_next_queue_row(cursor):
    # queue_table = 'eschol_api_queue_test' if test_mode else 'eschol_api_queue'
    queue_table = 'osti_submission_test' if test_mode else 'osti_submission_live'
    # next_queue_row_query = f"select * from {queue_table} where updated=0 order by id limit 1;"
    next_queue_row_query = f"""
        select id, eschol_id, osti_id
        from {queue_table} 
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

    if 199 < response.status_code < 300:
        pprint(response.text)
        raise RuntimeError("Non-2xx eSchol API response. Exiting")

    return response['data']['item']


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
def update_submission_row_success(cursor, row, response):
    update_time = datetime.now.strftime('%Y-%m-%d %H:%M:%S')
    queue_table = 'osti_submissions_test' if test_mode else 'osti_submissions_live'
    update_queue_row_query = f"""
        update {queue_table} set
        updated='{update_time}', eschol_api_response_code={response.status_code}
        where id={row['id']};"""
    cursor.execute(update_queue_row_query)
    print(f"{cursor.rowcount} row updated.")


# =======================================
def update_submission_row_fail(cursor, row, response):
    queue_table = 'osti_submissions_test' if test_mode else 'osti_submissions_live'
    update_time = datetime.now.strftime('%Y-%m-%d %H:%M:%S')
    update_queue_row_query = f"""
        update {queue_table} set
        updated='{update_time}', eschol_api_response_code={response.status_code},
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
# Stub for main
if __name__ == "__main__":
    main()
