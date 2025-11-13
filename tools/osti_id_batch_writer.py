import boto3
import pymysql
import requests
from pprint import pprint

test_mode = True

queue_update_query = """
INSERT INTO eschol_api_queue
SELECT
	id,
	osti_id,
	elements_id,
    eschol_id,
	0 as `updated`
from
	osti_submissions_live
where
	media_response_code between 200 and 299
	and osti_id not in (
		select eaq.osti_id
		from eschol_api_queue eaq);
"""


# =======================================
# More-or-less lifted from program_setup.py
def get_creds():
    # Get AWS session
    session = boto3.Session()

    def get_ssm_parameters(folder, names):
        ssm_client = session.client(service_name='ssm', region_name='us-west-2')

        param_names = [f"{folder}/{name}" for name in names]
        response = ssm_client.get_parameters(Names=param_names, WithDecryption=True)

        param_values = {
            (param['Name'].split('/')[-1]): param['Value']
            for param in response['Parameters']}

        return param_values

    selected_creds = {}

    selected_creds['cdl_db'] = get_ssm_parameters(
        f"/pub-oapi-tools/tools-rds/prod",
        ['user', 'password', 'server', 'port', 'osti-db', 'driver', 'osti-table'])

    if test_mode:
        selected_creds['eschol_api'] = get_ssm_parameters(
            f"/pub-oapi-tools/eschol-api/qa",
            ['endpoint', 'priv-key', 'cookie'])
    else:
        selected_creds['eschol_api'] = get_ssm_parameters(
            f"/pub-oapi-tools/eschol-api/prod",
            ['endpoint', 'priv-key', 'cookie'])

    return selected_creds


# =======================================
def get_cdl_connection(mysql_creds):
    # connect to the mySql db
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
# Main
def main():
    creds = get_creds()
    mysql_conn = get_cdl_connection(creds['cdl_db'])

    with mysql_conn.cursor() as cursor:
        print("Queueing new OSTI submissions...")
        cursor.execute(queue_update_query)
        print(f"{cursor.rowcount} rows enqueued.")
        run_updates(cursor, creds['eschol_api'])

    mysql_conn.close()


# =======================================
# Main update loop
def run_updates(cursor, eschol_creds):
    print("Running looped updates.\n")
    queue_table = 'eschol_api_queue_test' if test_mode else 'eschol_api_queue'

    print("Querying for next row...")
    get_next_queue_row = f"select * from {queue_table} where updated=0 limit 1;"

    cursor.execute(get_next_queue_row)
    row = cursor.fetchone()
    print(row)

    update_eschol_api(row, eschol_creds)

    if not test_mode:
        print("Updating queue row...")
        update_queue_row = f"update {queue_table} set updated=1 where id={row['id']};"
        cursor.execute(update_queue_row)
        print(f"{cursor.rowcount} row updated.")


# =======================================
def update_eschol_api(row, creds):
    test_query = 'query getItem($input_id: ID!){ item(id:$input_id) { id, title, rights } }'
    row['eschol_id'] = 'qtttrmz60v';
    print(f"escholID: {row['eschol_id']}")
    item_vars = {'input_id': f"ark:/13030/{row['eschol_id']}"}

    # Set headers cookies
    headers = dict(PRIVILEGED=creds['priv-key'])
    cookies = dict(ACCESS_COOKIE=creds['cookie']) if test_mode else {}

    # Send the req
    response = requests.post(
        url=creds['endpoint'],
        headers=headers,
        cookies=cookies,
        json={"query": test_query,
              "variables": item_vars})

    # Print response
    print(f"Response: {response.status_code} -- {response.reason}")
    print(response)
    print(response.text)
    if response.status_code != 200:
        print(response.text)
        print("----------------------------------------")


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
