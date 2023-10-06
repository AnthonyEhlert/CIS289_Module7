"""
Program Name: Update_Delete_DB_Data_Ehlert.py
Author: Tony Ehlert
Date: 10/5/2023

Program Description:  This program uses an existing database that contains tables and information about weather
and reads and modifies the records, while also removing one of the cities and all data associated with it.
"""
import datetime
import sqlite3

import pandas as pd
from sqlite3 import Error


def create_connection(db_file):
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


if __name__ == '__main__':
    #### Create a copy of your database (You can do this in Python or whatever way you prefer)
    #### If you used inches for original precip, read them in from db & replace w/ mm.  Do opposite if you started w/ mm.

    # create variables needed to create conn to db and to run statements on db
    database = 'weather_tracking.db'
    conn = create_connection(database)
    cur = conn.cursor()

    # read in original 'precipitation' column
    with sqlite3.connect(database) as db:
        precip_in_inches = conn.execute("SELECT precipitation, county FROM Precipitation").fetchall()
    # print(precip_in_inches)

    # create new empty list of tuples to hold converted precipitation
    precip_in_mm = []

    # convert inches to mm and create tuple (mm, inches, county) for each record
    for incrementer in range(0, len(precip_in_inches)):
        precip_in_mm.append((round(precip_in_inches[incrementer][0] * 25.4, 2), precip_in_inches[incrementer][0],
                             precip_in_inches[incrementer][1]))
    # print(precip_in_mm)

    # put converted data back into db by creating composite key of 'precipitation' AND 'county'
    with sqlite3.connect(database) as db:
        conn.executemany("UPDATE Precipitation SET precipitation=? WHERE precipitation=? AND county=?", (precip_in_mm))
    conn.commit()

    #### Make date on ea. reading 1 day earlier (because you realized reading date was actually day after actual precip)

    # read in original 'date' column
    with sqlite3.connect(database) as db:
        old_date = cur.execute("SELECT date, county FROM Precipitation").fetchall()
    # print(type(old_date[0][0]))

    # create new empty list of tuples to hold converted date
    new_date = []

    # subtract one day and create tuple (str_day, old_day, county) for each record
    for incrementer in range(0, len(old_date)):
        # cast date to new variable of datetime datatype
        datetime_date = datetime.datetime.strptime(old_date[incrementer][0], '%m/%d/%Y')

        # subtract one day from datetime_date
        day = datetime_date - datetime.timedelta(days=1)

        # convert new_date object back into string object
        str_day = day.strftime('%#m/%#d/%Y')

        # create tuple (str_day, old_day, county) and append to new_date list of tuples
        new_date.append((str_day, old_date[incrementer][0], old_date[incrementer][1]))
    # print(new_date)

    # put converted data back into db by creating composite key of 'date' AND 'county'
    with sqlite3.connect(database) as db:
        conn.executemany("UPDATE Precipitation SET date=? WHERE date=? AND county=?", new_date)
    conn.commit()

    #### Delete one of your cities and all of the data associated with it.

    # first need to delete and records containing 'Worth' from Precipitation table, else violate foreign key constraint
    conn.execute("DELETE FROM Precipitation WHERE county='Worth'")

    # now delete 'Worth' from Location table
    conn.execute("DELETE FROM Location WHERE county='Worth'")

    # commit changes and close conn
    conn.commit()
    conn.close()
