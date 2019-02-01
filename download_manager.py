import postgres_script as pgs
import variables
import psycopg2

def postgres_connect():
    connect_str = "dbname='{}' user='{}' host='{}' password='{}'".format(variables.POSTGRES_DB, variables.POSTGRES_USER, variables.POSTGRES_HOST, variables.POSTGRES_PASSWORD)
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()

    return conn, cursor

def get_database_data():
    try:
        conn, cursor = postgres_connect()
        cursor.execute(pgs.GET_ALL_QUERY)
        records_list = cursor.fetchall()

        # print(records_list)
        # for record in records_list:
        #     print("ID: {}".format(record[0]))
        #     print("Bucket: {}".format(record[1]))
        #     print("File Name: {}".format(record[2]))
        #     print("Created On: {}".format(record[3]))
        #     print("Created By: {}".format(record[4]))
        #     print("\n")

        return records_list
    except Exception as e:
        print(e)
        return None