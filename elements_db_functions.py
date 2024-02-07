import pyodbc
import test_output



# --------------------------
# Query the Elements DB, find pubs which need to be sent.
def get_new_osti_pubs(sql_creds, osti_eschol_db, args):

    # Load SQL file
    try:
        # sql_file = open("sql_files/get_new_osti_pubs_with_json.sql")
        sql_file = open("sql_files/get_new_osti_pubs_with_json_temp_table.sql")
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

    print("Creating temp table with submitted OSTI data.")
    temp_table_query = create_submitted_temp_table(osti_eschol_db)

    if args.test:
        test_output.output_temp_table_query(temp_table_query)

    # Execute the temp table query in Elements
    cursor.execute(temp_table_query)
    if args.test:
        test_output.output_temp_table_results(get_full_temp_table(cursor))

    print("Executing query to retrieve new OSTI pubs.")
    cursor.execute(sql_query)

    # pyodbc doesn't return dicts automatically, we have to make them ourselves
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return rows


# --------------------------
def create_submitted_temp_table(osti_eschol_db):
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

