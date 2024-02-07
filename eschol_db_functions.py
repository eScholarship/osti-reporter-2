# pyMySQL - https://pymysql.readthedocs.io/en/latest/
import pymysql


# ------------------------------
def get_osti_db(mysql_creds):

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
