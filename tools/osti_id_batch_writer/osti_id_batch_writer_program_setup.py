import boto3
import pymysql


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
