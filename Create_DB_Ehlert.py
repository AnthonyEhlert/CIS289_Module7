"""
Program Name: Create_DB_Ehlert.py
Author: Tony Ehlert
Date: 10/5/2023

Program Description: This program creates a SQLite database names "weather_tracking.db"
and then creates and adds two tables to that database named "Precipitation" and "Location".  After creating and adding
the tables, the database information is printed to the console.
"""
import sqlite3
from sqlite3 import Error


def creat_connection(db_file):
    """
    This function creates a database connection to a SQLite database that is specified by the param
    :param db_file: database file
    :return: Connection object if successful, else None
    """

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
        return conn


def create_table(conn, create_tbl_sql):
    """
    This function creates a database table using the conn and the create_tbl_sql statement passed in
    :param conn: Connection object
    :param create_tbl_sql: CREATE TABLE sql statement
    :return:
    """
    try:
        cur = conn.cursor()
        cur.execute(create_tbl_sql)
    except Error as e:
        print(e)


if __name__ == "__main__":
    #### After importing the data integration module create a database called weather_tracking
    database = "weather_tracking.db"

    #### In the database, create a table called Precipitation
    # Table should have the columns: location (foreign key to county in Location table), date (text), precipitation (float), precip_type(text)
    sql_create_precipitation_tbl = """ CREATE TABLE IF NOT EXISTS Precipitation (
                                        county text,
                                        date text,
                                        precipitation real,
                                        precip_type text,
                                        FOREIGN KEY (county)
                                            REFERENCES Location (county)
                                        );"""

    #### In the database, create another table called Location
    # The table should have the columns county (primary key)(text), state (text)
    sql_create_location_table = """CREATE TABLE IF NOT EXISTS Location (
                                    county text PRIMARY KEY,
                                    state text
                                );"""

    # create db connection
    conn = creat_connection(database)

    # create tables
    if conn is not None:
        # create Location table first as it contains the primary key that is utilized as a foreign key
        create_table(conn, sql_create_location_table)

        # create Precipitation table
        create_table(conn, sql_create_precipitation_tbl)
    else:
        print("Error! Cannot create connection to the database.")

    #### Output db info in the console with your program (table names and column names)
    with sqlite3.connect('weather_tracking.db') as db:
        cur = db.cursor()
        new_line_indent = "\n   "

        result = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = sorted(list(zip(*result))[0])
        print("tables are:" + new_line_indent + new_line_indent.join(table_names))

        for table_name in table_names:
            result = cur.execute("PRAGMA table_info('%s')" % table_name).fetchall()
            column_names = list(zip(*result))[1]
            print(("\n column names for %s:" % table_name) + new_line_indent + (new_line_indent.join(column_names)))
