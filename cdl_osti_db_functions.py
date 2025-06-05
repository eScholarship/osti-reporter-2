# pyMySQL - https://pymysql.readthedocs.io/en/latest/
import pymysql
from time import sleep


# Note: mysql_creds are set individually for read and write, So this
#       connection should be called before each individual mysql_operation.
def get_cdl_connection(mysql_creds):
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


# Retrieves the entire eSchol OSTI db
def get_cdl_osti_db(mysql_creds):
    mysql_conn = get_cdl_connection(mysql_creds)

    # Load .sql file
    try:
        sql_file = open("sql_files/get_osti_db_from_eschol.sql")
        sql_query = sql_file.read()
        sql_query = sql_query.replace("table_replace", mysql_creds['table'])

    except Exception as e:
        print("ERROR WHILE READING OR OPENING .SQL FILE.")
        raise e

    # Open cursor and send query
    with mysql_conn.cursor() as cursor:
        print("Connected to eSchol MySQL DB. Getting osti_eschol db.")
        cursor.execute(sql_query)
        eschol_osti_db = cursor.fetchall()

    return eschol_osti_db


# Adds new successful OSTI submissions to the eSchol OSTI db.
def insert_new_metadata_submissions(new_metadata_submissions, mysql_creds):
    mysql_conn = get_cdl_connection(mysql_creds)

    # Build the SQl query
    insert_query = (f"""INSERT INTO {mysql_creds['table']} 
        (date_stamp,
        eschol_ark,
        osti_id,
        doi,
        lbnl_report_no,
        elements_id,
        eschol_id,
        eschol_pr_modified_when
        ) VALUES \n""")

    converted_pubs = [convert_nulls_for_sql(pub) for pub in new_metadata_submissions]

    values_list = [(f"""(CURDATE(),
                    '{pub['ark']}',
                    {pub['osti_id']},
                    '{pub['doi']}',
                    '{pub['LBL Report Number']}',
                    {pub['id']},
                    '{pub['eSchol ID']}',
                    '{pub['eschol_pr_modified_when'].strftime('%Y-%m-%d %H:%M:%S.%f')}')"""
                    ).replace("'Null'", 'Null') for pub in converted_pubs]

    insert_query += (",\n".join(values_list)) + ";"

    # Open cursor and send query
    with mysql_conn.cursor() as cursor:
        cursor.execute(insert_query)
        mysql_conn.commit()

    mysql_conn.close()


# Adds PDF data for new osti submissions. Saves failures as well as successes.
def update_osti_db_with_pdfs(new_pdf_submissions, mysql_creds):
    mysql_conn = get_cdl_connection(mysql_creds)

    with mysql_conn.cursor() as cursor:
        for pub in new_pdf_submissions:
            pub = convert_nulls_for_sql(pub)

            print(f"Updating Elements ID:{pub['id']}, OSTI ID:{pub['osti_id']} with media response.")
            update_query = (f"""UPDATE {mysql_creds["table"]} SET 
                            media_response_code={pub['media_response_code']},
                            media_id={pub['media_id']},
                            media_file_id={pub['media_file_id']},
                            prf_filename='{pub['Filename']}',
                            prf_size={pub['File Size']}
                            WHERE osti_id={pub['osti_id']}; """
                            ).replace("'Null'", 'Null')

            cursor.execute(update_query)
            mysql_conn.commit()
            sleep(3)


# Metadata updates: If the update was successful in OSTI,
# write the update back to the eSchol OSTI DB.
def update_osti_db_metadata(successful_metadata_updates, mysql_creds):
    mysql_conn = get_cdl_connection(mysql_creds)

    with mysql_conn.cursor() as cursor:
        for pub in successful_metadata_updates:
            print(f"Updating Elements ID:{pub['id']}, OSTI ID:{pub['osti_id']} with new metadata.")

            pub = convert_nulls_for_sql(pub)
            update_query = (f"""UPDATE {mysql_creds["table"]} SET
                            eschol_ark='{pub['ark']}',
                            doi='{pub['doi']}',
                            lbnl_report_no='{pub['LBL Report Number']}',
                            elements_id={pub['id']},
                            eschol_id='{pub['eSchol ID']}',
                            eschol_pr_modified_when='{pub['eschol_pr_modified_when'].strftime('%Y-%m-%d %H:%M:%S.%f')}'
                            WHERE osti_id={pub['osti_id']}; """).replace("'Null'", 'Null')

            cursor.execute(update_query)
            mysql_conn.commit()
            sleep(3)


def convert_nulls_for_sql(pub):
    converted_pub = {}

    for k, v in pub.items():
        if v is None or v == "None" or v == "":
            converted_pub[k] = "Null"
        else:
            converted_pub[k] = pub[k]

    return converted_pub
