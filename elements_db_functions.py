import pyodbc
import write_logs


# --------------------------
# Query the Elements DB, find pubs which need to be sent.
def get_new_osti_pubs(sql_creds, temp_table_query, args, log_folder):

    # Load SQL file
    try:
        # sql_file = open("sql_files/get_new_osti_pubs_with_json.sql")
        sql_file = open("sql_files/get_new_osti_pubs_from_elements.sql")
        sql_query = sql_file.read()

    except Exception as e:
        print("ERROR WHILE HANDLING SQL FILE. The file was unable to be located, \
                or a problem occurred while reading its contents.")
        raise e

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
    cursor = conn.cursor()

    # Execute the temp table query in Elements
    cursor.execute(temp_table_query)

    # Log the Elements temp table output.
    write_logs.output_temp_table_results(log_folder, get_full_temp_table(cursor))

    # Elements query: Replace variable definitions with appropriate URLS
    sql_query = replace_url_variable_values(args.input_qa, sql_query)

    print("Executing query to retrieve new OSTI pubs.")
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


# --------------------------
def create_submitted_temp_table(osti_eschol_db):
    print("Creating temp table with submitted OSTI data.")

    temp_table = '''
BEGIN TRANSACTION 
CREATE TABLE #osti_submitted
(doi VARCHAR(80), eschol_id VARCHAR(80));
COMMIT TRANSACTION
GO
'''

    transaction_header = '''
BEGIN TRANSACTION
INSERT INTO #osti_submitted
(doi, eschol_id)
VALUES
'''

    transaction_footer = '''
COMMIT TRANSACTION
GO
'''

    temp_table += transaction_header
    value_strings = []
    insert_limit = 500

    for index, row in enumerate(osti_eschol_db, 1):

        # Skip any null DOIs
        if not row['doi']:
            value_strings.append("(NULL, '" + row['eschol_id'] + "')")
        else:
            value_strings.append("('" + row['doi'] + "', '" + row['eschol_id'] + "')")

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
def get_full_temp_table(cursor):
    cursor.execute("SELECT * FROM #osti_submitted;")
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return rows


# --------------------------
def replace_url_variable_values(input_qa, sql_query):

    if input_qa:
        sql_query = sql_query.replace(
            'ELEMENTS_PUB_URL_REPLACE',
            'https://qa-oapolicy.universityofcalifornia.edu/viewobject.html?cid=1&id=')
        sql_query = sql_query.replace(
            'ESCHOL_FILES_URL_REPLACE',
            'https://pub-jschol2-stg.escholarship.org/content/')

    else:
        sql_query = sql_query.replace(
            'ELEMENTS_PUB_URL_REPLACE',
            'https://oapolicy.universityofcalifornia.edu/viewobject.html?cid=1&id=')
        sql_query = sql_query.replace(
            'ESCHOL_FILES_URL_REPLACE',
            'https://escholarship.org/content/')

    return sql_query
