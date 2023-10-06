"""
Program Name: Create_Read_DB_Data_Ehlert.py
Author: Tony Ehlert
Date: 10/5/2023

Program Description:  This program uses an existing empty database with two tables that are meant to store information
about weather and then imports data from .csv files into the database tables.  After the data in written to the tables
the data is then read from the tables and put into a dataframe in which a visual about precipitation data is created.
"""
import csv
import sqlite3

import matplotlib.pyplot as plt
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
    database = 'weather_tracking.db'
    conn = create_connection(database)
    cur = conn.cursor()

    # delete any existing records from tables in database to enable multiple runs of code w/out adding dup records
    cur.execute("DELETE FROM Precipitation")
    cur.execute("DELETE FROM Location")
    conn.commit()

    #### Gather weather data from at least two different counties for a range of 30 days

    #### Import & Write this data to your database tables in the Location and Precipitation tables
    with open('weather_data.csv', 'r') as input_file:
        # create data variable to hold contents of .csv file
        data = csv.DictReader(input_file)

        # create a list of tuples (ea. tuple is a record) from contents of data variable
        to_db_precip = [(i['county'], i['date'], i['precipitation'], i['precip_type']) for i in data]

    with open('weather_data.csv', 'r') as input_file:
        # create data variable to hold contents of .csv file
        data = csv.DictReader(input_file)

        # create a list of tuples (ea. tuple is a record) from data variable containing records read from .csv
        to_db_loc = [(i['county'], i['state']) for i in data]

    cur.executemany("REPLACE INTO Location (county, state) VALUES (?,?)", to_db_loc)
    cur.executemany("REPLACE INTO Precipitation (county, date, precipitation, precip_type) VALUES (?,?,?,?)",
                    to_db_precip)

    conn.commit()
    conn.close()

    #### Now bring this data back into your program to create a visual of the precipitation data
    precip_df = pd.read_sql_query("SELECT * FROM Precipitation", sqlite3.connect('weather_tracking.db'))
    # print(precip_df.to_string())

    precip_df['date'] = pd.to_datetime(precip_df['date'])
    # print(precip_df['date'].dtype)

    data_wayne_co = precip_df[precip_df['county'] == 'Wayne']
    sorted_data_wayne_co = data_wayne_co.sort_values('date').copy()
    plot_y_wayne_co = list(sorted_data_wayne_co['precipitation'])
    # print(sorted_data_wayne_co)

    data_worth_co = precip_df[precip_df['county'] == 'Worth']
    sorted_data_worth_co = data_worth_co.sort_values('date').copy()
    plot_y_worth_co = list(sorted_data_worth_co['precipitation'])
    # print(sorted_data_worth_co)

    plot_x_date = list(data_worth_co['date'].dt.day)
    # print(plot_x_date)

    ax = plt.subplot()

    ax.plot(plot_x_date, plot_y_worth_co, color='g', label='Worth County, IA')
    ax.plot(plot_x_date, plot_y_wayne_co, color='purple', label='Wayne County, IA')
    ax.axes.set_xticks(plot_x_date)
    ax.axes.set_xlabel('Day of Month')
    ax.axes.set_xticklabels(plot_x_date, rotation=45, fontsize=7)
    ax.axes.set_ylabel("Inches of Precipitation")
    ax.legend(loc='upper left')
    ax.title.set_text('June 2023 Precipitation for Worth and Wayne County. IA')
    plt.show()
