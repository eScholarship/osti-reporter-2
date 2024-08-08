# pyMySQL - https://pymysql.readthedocs.io/en/latest/
import pymysql


# ------------------------------
def get_eschol_osti_db(mysql_creds):
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

    # Load SQL file
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

    mysql_conn.close()

    return eschol_osti_db


# ------------------------------
def update_eschol_osti_db(new_osti_pubs, mysql_creds):
    # Add only successfully-submitted pubs to the db.
    successful_submissions = [pub for pub in new_osti_pubs if pub['response_success'] is True]

    if len(successful_submissions) == 0:
        print("No successful submissions in this set of publications. Exiting program.")
        exit(0)

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

    # Build the SQl query -- MODIFICATIONS FOR UPDATES PHASE 3
    # insert_query = ("""INSERT INTO osti_eschol
    #     (date_stamp, eschol_ark, osti_id, doi, lbnl_report_no,
    #     pr_modified_when, prf_filename, prf_size, python_rewrite) VALUES \n""")

    # Build the SQl query -- FOR EXISTING PARITY
    insert_query = ("INSERT INTO " + mysql_creds["table"] + """
        (date_stamp, eschol_ark, osti_id, media_file_id, doi, lbnl_report_no, elements_id, eschol_id) VALUES \n""")

    values_list = [
        ('(CURDATE(), "%s", %s, %s, "%s", "%s", %s, "%s")' % (
            pub['ark'], pub['osti_id'], pub['media_file_id'],
            pub['doi'], pub['LBL Report Number'], pub['id'], pub['eSchol ID'])
         ).replace('"None"', 'Null')
        for pub in successful_submissions]

    insert_query += (",\n".join(values_list)) + ";"

    # Open cursor and send query
    with mysql_conn.cursor() as cursor:
        print("Connected to eSchol MySQL DB. Inserting successful submissions into eschol_osti.\n")
        print(insert_query + "\n")
        cursor.execute(insert_query)
        mysql_conn.commit()

    mysql_conn.close()
