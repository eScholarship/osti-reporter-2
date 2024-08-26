import pyodbc
import write_logs


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
def create_temp_table_in_elements(conn, temp_table_query):
    cursor = conn.cursor()
    cursor.execute(temp_table_query)


# --------------------------
# Query the Elements DB, find pubs which need to be sent.
def get_new_osti_pubs(conn, args):

    # Load SQL file
    try:
        sql_file = open("sql_files/get_new_osti_pubs_from_elements.sql")
        sql_query = sql_file.read()
    except Exception as e:
        print("ERROR WHILE OPENING OR READING SQL FILE.")
        raise e

    print("Executing query to retrieve new OSTI pubs.")
    sql_query = replace_url_variable_values(args.input_qa, sql_query)
    cursor = conn.cursor()
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


# --------------------------
def generate_temp_table_sql(osti_eschol_db):
    print("Creating temp table with submitted OSTI data.")

    temp_table = '''
        BEGIN TRANSACTION 
        CREATE TABLE #osti_submitted (
            osti_id INT,
            elements_id INT,
            doi VARCHAR(80),
            eschol_id VARCHAR(80),
            eschol_pr_modified_when DATETIME,
            prf_filename VARCHAR(200),
            prf_size BIGINT,
            media_response_code INT,
            media_id INT,
            media_file_id INT
        );
        COMMIT TRANSACTION
        GO;
        '''

    transaction_header = '''
        BEGIN TRANSACTION
        INSERT INTO #osti_submitted (
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
        VALUES
        '''

    transaction_footer = '''
        COMMIT TRANSACTION
        GO;
        '''

    temp_table += transaction_header
    value_strings = []
    insert_limit = 500

    for index, row in enumerate(osti_eschol_db, 1):

        # MSSQL only accepts datetime/timestamp with 3 digits of fractional time
        if row['eschol_pr_modified_when'] is not None:
            row['eschol_pr_modified_when'] = row['eschol_pr_modified_when'].strftime('%Y-%m-%d %H:%M:%S.%f')
            row['eschol_pr_modified_when'] = row['eschol_pr_modified_when'][:-3]

        # https://stackoverflow.com/questions/29380383/python-pypyodbc-row-insert-using-string-and-nulls/29419430#29419430

        row = convert_nulls_for_sql(row)
        value_strings.append(
            (f"""(
            {row['osti_id']},
            {row['elements_id']},
            '{row['doi']}',
            '{row['eschol_id']}',
            '{row['eschol_pr_modified_when']}',
            '{row['prf_filename']}',
            {row['prf_size']},
            {row['media_response_code']},
            {row['media_id']},
            {row['media_file_id']})"""
             ).replace("'Null'", 'Null')
        )

        if index % insert_limit == 0:
            temp_table += ",\n".join(value_strings)
            value_strings = []
            temp_table += transaction_footer
            temp_table += transaction_header

        elif index == (len(osti_eschol_db)):
            temp_table += ",\n".join(value_strings)
            temp_table += transaction_footer

    return temp_table


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
# Get OSTI-submitted items who've had metadata updates.
def get_osti_metadata_updates(conn, args):

    # Load SQL file
    try:
        sql_file = open("sql_files/get_updated_metadata_from_elements.sql")
        sql_query = sql_file.read()
    except Exception as e:
        print("ERROR WHILE OPENING OR READING SQL FILE")
        raise e

    print("Executing query to retrieve updated OSTI pub metadata.")
    sql_query = replace_url_variable_values(args.input_qa, sql_query)
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
    try:
        sql_file = open("sql_files/get_updated_pdfs_from_elements.sql")
        sql_query = sql_file.read()
    except Exception as e:
        print("ERROR WHILE OPENING OR READING SQL FILE.")
        raise e

    print("Executing query to retrieve updated OSTI pub PDFs.")
    sql_query = replace_url_variable_values(args.input_qa, sql_query)
    cursor = conn.cursor()
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


def convert_nulls_for_sql(pub):
    converted_pub = {}

    for k, v in pub.items():
        if v is None or v == "None" or v == "":
            converted_pub[k] = "Null"
        else:
            converted_pub[k] = pub[k]

    return converted_pub
