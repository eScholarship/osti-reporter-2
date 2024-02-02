import pyodbc


# --------------------------
# Query the Elements DB, find pubs which need to be sent.
def get_new_osti_pubs(sql_creds, elink_version):

    # Load SQL file
    try:
        # sql_file = open("sql_files/get_new_osti_pubs_with_json.sql")
        sql_file = open("sql_files/testing-query.sql")
        sql_query = sql_file.read()
    except:
        raise Exception("ERROR WHILE HANDLING SQL FILE. The file was unable to be located, \
                or a problem occurred while reading its contents.")

    # Connect to db
    try:
        conn = pyodbc.connect(
            driver=sql_creds['driver'],
            server=(sql_creds['server'] + ',' + sql_creds['port']),
            database=sql_creds['database'],
            uid=sql_creds['user'],
            pwd=sql_creds['password'],
            trustservercertificate='yes')

    except:
        raise Exception("ERROR CONNECTING TO DATABASE. Check credits and/or SSH tunneling.")

    # Create cursor, execute query
    print("Connected to Elements reporting DB. Getting new osti publications.")
    conn.autocommit = True  # Required when queries use TRANSACTION
    cursor = conn.cursor()
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows
