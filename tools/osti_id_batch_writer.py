import boto3
import pymysql
from pprint import pprint

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

    selected_creds['eschol_api'] = get_ssm_parameters(
        f"/pub-oapi-tools/eschol-api/qa",
        ['endpoint', 'priv_key', 'cookie'])

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

        run_updates(cursor)

    mysql_conn.close()


# =======================================
# Main update loop
def run_updates(cursor):
    print("Querying for next row...")
    get_next_queue_row = "select * from eschol_api_queue_test where updated=0 limit 1;"
    cursor.execute(get_next_queue_row)
    row = cursor.fetchone()
    print(row)





    print("Updating queue row...")
    update_queue_row = f"update eschol_api_queue_test set updated=0 where id={row['id']};"
    cursor.execute(update_queue_row)
    print(f"{cursor.rowcount} row updated.")

# =======================================
# Stub for main
if __name__ == "__main__":
    main()
