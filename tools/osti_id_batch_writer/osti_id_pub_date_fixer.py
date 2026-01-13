import boto3
import pymysql
import requests
from pprint import pprint
from datetime import datetime
from time import sleep

# Global vars
test_mode = False
verbose_mode = True
reset_mode = False
verify_updates = False
error_reset = 3
error_count = error_reset
total_updates = 1000
sleep_time = 1


def main():
    creds = get_creds()

    cdl_conn = get_cdl_connection(creds['cdl_db'])
    osti_table = creds['cdl_db']['osti-table-test'] \
        if test_mode else creds['cdl_db']['osti-table']

    with cdl_conn.cursor() as cdl_cursor:
        print("Querying osti submissions for the next row to update.")
        rows_to_fix_query = f"""
        SELECT eschol_id, pub_date
        FROM {osti_table}
        WHERE
            eschol_api_updated IS NOT NULL
            AND pub_date IS NOT NULL
            AND pub_date_fixed IS NULL
        ORDER BY id asc
        LIMIT {total_updates};
        """

        cdl_cursor.execute(rows_to_fix_query)
        rows_to_fix = cdl_cursor.fetchall()
        cdl_cursor.close()

    eschol_conn = get_eschol_connection(creds['eschol_db'])

    for row in rows_to_fix:
        print(row)
        update_eschol_db(eschol_conn, row)
        update_cdl_db(cdl_conn, row, osti_table)

        sleep(sleep_time)

    cdl_conn.close()
    eschol_conn.close()


# =======================================
def update_eschol_db(eschol_conn, row):
    if verbose_mode:
        print(f"Updating: {row['eschol_id']}")

    with eschol_conn.cursor() as eschol_cursor:
        update_query = f"""
            update items
            set published = '{row['pub_date']}'
            where id = '{row['eschol_id']}'"""
        eschol_cursor.execute(update_query)


# =======================================
def update_cdl_db(cdl_conn, row, osti_table):
    update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with cdl_conn.cursor() as cdl_cursor:
        update_query = f"""
            update {osti_table}
            set pub_date_fixed = '{update_time}'
            where eschol_id = '{row['eschol_id']}'"""
        cdl_cursor.execute(update_query)


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
# connect to the mySql db
def get_eschol_connection(mysql_creds):
    try:
        mysql_conn = pymysql.connect(
            host=mysql_creds['server'],
            user=mysql_creds['user'],
            password=mysql_creds['password'],
            database=mysql_creds['database'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True)

        return mysql_conn
    except Exception as e:
        print("ERROR WHILE CONNECTING TO MYSQL DATABASE.")
        raise e

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
        creds['eschol_db'] = get_ssm_parameters(
            f"/pub-oapi-tools/eschol-db/qa",
            ['database', 'server', 'user', 'password'])
    else:
        creds['eschol_api'] = get_ssm_parameters(
            f"/pub-oapi-tools/eschol-api/prod",
            ['endpoint', 'priv-key', 'cookie'])
        creds['eschol_db'] = get_ssm_parameters(
            f"/pub-oapi-tools/eschol-db/prod",
            ['database', 'server', 'user', 'password'])

    return creds


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
