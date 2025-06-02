import pyodbc


def get_elements_connection(sql_creds):
    # Connect to db
    try:
        conn = pyodbc.connect(
            driver=sql_creds['driver'],
            server=(sql_creds['server'] + ',' + sql_creds['port']),
            database=sql_creds['database'],
            uid=sql_creds['user'],
            pwd=sql_creds['password'],
            trustservercertificate='yes')

    except Exception as e:
        print("ERROR CONNECTING TO ELEMENTS DATABASE. Check credits and/or SSH tunneling.")
        raise e

    print("Connected to Elements reporting DB.")
    conn.autocommit = True  # Required when queries use TRANSACTION

    return conn


# --------------------------
# Make the temp table in elements
def create_temp_table_in_elements(conn, osti_submitted_db):

    # Helper -- returns an array of arrays of the specified size
    def get_osti_eschol_chunks(osdb, chunk_size=500):
        chunks = []
        while len(osdb) >= chunk_size:
            chunk = osdb[:chunk_size]
            chunks.append(chunk)
            del osdb[:chunk_size]
        chunks.append(osdb)
        return chunks

    # Helper -- MSSQL only accepts datetime/timestamp with 3 digits of fractional time
    def format_datetime_for_mssql(dt):
        if dt is not None:
            dt = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            dt = dt[:-3]
        return dt

    # Main work starts here
    print("Creating temp table with submitted OSTI data.")

    # Load SQL file
    with open("sql_files/create_temp_table_in_elements.sql") as f:
        create_temp_table_sql = f.read()

    cursor = conn.cursor()
    cursor.execute(create_temp_table_sql)

    cursor.fast_executemany = True  # enables bulk inserting in executemany
    osti_submitted_db_chunks = get_osti_eschol_chunks(osti_submitted_db)
    for i, chunk in enumerate(osti_submitted_db_chunks, 1):
        print(f"inserting chunk {i} / {len(osti_submitted_db_chunks)}")

        insert_sql = '''INSERT INTO #osti_submitted (
            osti_id,
            elements_id,
            doi,
            eschol_id,
            eschol_pr_modified_when,
            prf_filename,
            prf_size,
            media_response_code,
            media_id,
            media_file_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''

        insert_values = [
            [row['osti_id'],
             row['elements_id'],
             row['doi'],
             row['eschol_id'],
             format_datetime_for_mssql(row['eschol_pr_modified_when']),
             row['prf_filename'],
             row['prf_size'],
             row['media_response_code'],
             row['media_id'],
             row['media_file_id']
             ] for row in chunk]

        cursor.executemany(insert_sql, insert_values)


# --------------------------
# Query the Elements DB, find pubs which need to be sent.
def get_new_osti_pubs(conn, args):
    cursor = conn.cursor()

    # Load SQL file
    with open("sql_files/get_new_osti_pubs_from_elements.sql") as f:
        sql_query = f.read()

    print("Executing query to retrieve new OSTI pubs.")
    sql_query = replace_url_variable_values(args.input_qa, sql_query)
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


# --------------------------
def get_full_temp_table(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM #osti_submitted;")
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return rows


# --------------------------
def replace_url_variable_values(input_qa, sql_query):
    if input_qa:
        sql_query = sql_query.replace('ELEMENTS_PUB_URL_REPLACE',
                                      'https://qa-oapolicy.universityofcalifornia.edu/viewobject.html?cid=1&id=')
        sql_query = sql_query.replace('ESCHOL_FILES_URL_REPLACE',
                                      'https://pub-jschol2-stg.escholarship.org/content/')
    else:
        sql_query = sql_query.replace('ELEMENTS_PUB_URL_REPLACE',
                                      'https://oapolicy.universityofcalifornia.edu/viewobject.html?cid=1&id=')
        sql_query = sql_query.replace('ESCHOL_FILES_URL_REPLACE',
                                      'https://escholarship.org/content/')

    return sql_query


# --------------------------
def add_individual_update_where_clause(id_list, sql_query):
    print(f"Individual pubs specified for updates: {id_list}")

    id_list = [str(i) for i in id_list]
    id_list_joined = ', '.join(id_list)
    where_clause = f"AND p.[ID] in ({id_list_joined})"

    sql_query = sql_query.replace("-- INDIVIDUAL UPDATES PUB ID LIST REPLACE", where_clause)
    return sql_query


# --------------------------
# Get OSTI-submitted items who've had metadata updates.
def get_osti_metadata_updates(conn, args):

    # Load SQL file
    with open("sql_files/get_updated_metadata_from_elements.sql") as f:
        sql_query = f.read()

    print("Adjusting SQL query for specified db input.")
    sql_query = replace_url_variable_values(args.input_qa, sql_query)

    if args.individual_updates:
        sql_query = add_individual_update_where_clause(args.individual_updates, sql_query)

    print("Executing query to retrieve updated OSTI pub metadata.")
    cursor = conn.cursor()
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


# --------------------------
# Get OSTI-submitted items whose PDFs have been re-deposited
def get_osti_media_updates(conn, args):

    # Load SQL file
    with open("sql_files/get_updated_pdfs_from_elements.sql") as f:
        sql_query = f.read()

    print("Adjusting SQL query for specified db input.")
    sql_query = replace_url_variable_values(args.input_qa, sql_query)

    if args.individual_updates:
        sql_query = add_individual_update_where_clause(args.individual_updates, sql_query)

    print("Executing query to retrieve updated OSTI pub PDFs.")
    cursor = conn.cursor()
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows
