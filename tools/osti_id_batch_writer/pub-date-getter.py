import boto3
import pymysql
import pyodbc


def main():
    creds = get_creds()


def get_creds():
    # Get AWS session
    session = boto3.Session()

    def get_ssm_parameters(folder, names):
        print("Connect to SSM for parameters")
        ssm_client = session.client(service_name='ssm', region_name='us-west-2')

        param_names = [f"{folder}/{name}" for name in names]
        response = ssm_client.get_parameters(Names=param_names, WithDecryption=True)

        param_values = {
            (param['Name'].split('/')[-1]): param['Value']
            for param in response['Parameters']}

        return param_values

    # Arg switches
    input_cnx = "prod"
    creds = {
        'elements_reporting_db': get_ssm_parameters(
            f"/pub-oapi-tools/elements-reporting-db/{input_cnx}",
            ['user', 'password', 'server', 'port', 'database', 'driver']),
        'cdl_db_read': get_ssm_parameters(
            f"/pub-oapi-tools/tools-rds/{input_cnx}",
            ['user', 'password', 'server', 'port', 'osti-db', 'driver', 'osti-table'])}

    return creds


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
