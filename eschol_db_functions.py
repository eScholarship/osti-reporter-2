# pyMySQL - https://pymysql.readthedocs.io/en/latest/
import pymysql


def get_eschol_connection(mysql_creds):
    # connect to the mySql db
    try:
        mysql_conn = pymysql.connect(
            host=mysql_creds['host'],
            user=mysql_creds['user'],
            password=mysql_creds['password'],
            database=mysql_creds['database'],
            cursorclass=pymysql.cursors.DictCursor)

        return mysql_conn
    except Exception as e:
        print("ERROR WHILE CONNECTING TO MYSQL DATABASE.")
        raise e


# ------------------------------
def get_eschol_osti_db(mysql_creds):

    # Get the connection
    try:
        mysql_conn = pymysql.connect(
            host=mysql_creds['host'],
            user=mysql_creds['user'],
            password=mysql_creds['password'],
            database=mysql_creds['database'],
            cursorclass=pymysql.cursors.DictCursor)

    except Exception as e:
        print("ERROR WHILE CONNECTING TO MYSQL DATABASE.")
        raise e

    # Load .sql file
    try:
        sql_file = open("sql_files/get_osti_db_from_eschol.sql")
        sql_query = sql_file.read()
        sql_query = sql_query.replace("table_replace", mysql_creds['table'])
        print(sql_query)

    except Exception as e:
        print("ERROR WHILE HANDLING SQL FILE. The file was unable to be located, \
                or a problem occurred while reading its contents.")
        raise e

    # Open cursor and send query
    with mysql_conn.cursor() as cursor:
        print("Connected to eSchol MySQL DB. Getting osti_eschol db.")
        cursor.execute(sql_query)
        eschol_osti_db = cursor.fetchall()

    return eschol_osti_db


# ------------------------------
def update_eschol_osti_db(successful_submissions, mysql_creds):

    # connect to the mySql db
    try:
        mysql_conn = pymysql.connect(
            host=mysql_creds['host'],
            user=mysql_creds['user'],
            password=mysql_creds['password'],
            database=mysql_creds['database'],
            cursorclass=pymysql.cursors.DictCursor)
    except Exception as e:
        print("ERROR WHILE CONNECTING TO MYSQL DATABASE.")
        raise e

    # Build the SQl query
    insert_query = ("INSERT INTO " + mysql_creds["table"] + """
        (date_stamp,
        eschol_ark,
        osti_id,
        media_response_code,
        media_file_id,
        doi,
        lbnl_report_no,
        elements_id,
        eschol_id,
        eschol_pr_modified_when,
        prf_filename,
        prf_size
        ) VALUES \n""")

    values_list = [
        ('(CURDATE(), "%s", %s, %s, %s, "%s", "%s", %s, "%s", "%s", "%s", %s)' % (
            pub['ark'],
            pub['osti_id'],
            pub['media_response_code'],
            pub['media_file_id'],
            pub['doi'],
            pub['LBL Report Number'],
            pub['id'],
            pub['eSchol ID'],
            pub['eschol_pr_modified_when'].strftime('%Y-%m-%d %H:%M:%S.%f'),
            pub['Filename'],
            pub['File Size']
        )
         ).replace('"None"', 'Null').replace(', None', ', Null')
        for pub in successful_submissions]

    insert_query += (",\n".join(values_list)) + ";"

    # Open cursor and send query
    with mysql_conn.cursor() as cursor:
        print("Connected to eSchol MySQL DB. Inserting successful submissions into eschol_osti.\n")
        print(insert_query + "\n")
        cursor.execute(insert_query)
        mysql_conn.commit()

    mysql_conn.close()


# ------------------------------
def update_eschol_osti_db_updated_metadata(successful_submissions, mysql_creds):
    pass
