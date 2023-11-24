# import libraries
import os
import pandas as pd
import numpy as np
import gspread
import logging
from dotenv import load_dotenv
import mysql.connector as sql
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO)

# connect database
def connect_db(DATABASE, DB_USER, DB_PASSWORD, HOST):
    """
    """
    conn = sql.connect(
        database=DATABASE,
        user=DB_USER,
        host=HOST,
        password=DB_PASSWORD,
    )

    return conn

# create table
def create_table(connection):
    """
    """
    query = """ 
            CREATE TABLE housing_dataset(
            serial_no INT AUTO_INCREMENT,
            ID INT NOT NULL,
            loc VARCHAR(20) DEFAULT NULL,
            title VARCHAR(20) DEFAULT NULL,
            bedroom INT DEFAULT NULL,
            bathroom INT DEFAULT NULL,
            parking_space INT DEFAULT NULL,
            price FLOAT DEFAULT NULL,
            created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (serial_no)
    );
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)

            connection.commit()
            logging.info("table created successfully")
    except:
        logging.info("table already exists")
    
    return None

# load data into dataframe
def load_data(url, sheet_title):
    """
    """
    gc = gspread.service_account()
    sheet = gc.open_by_url(url)
    logging.info("opened url successfully")

    worksheet = sheet.worksheet(sheet_title)

    df = pd.DataFrame(worksheet.get_all_records())
    logging.info("data loaded into dataframe successfully")

    return df

# validate datatype
def validate_row(row):
    """
    """
    id = (type(row.iloc[0]) == int)
    loc = (type(row.iloc[1]) == str)
    title = (type(row.iloc[2]) == str)
    bedroom = (type(row.iloc[3]) == int)
    bathroom = (type(row.iloc[4]) == int)
    parking_space = (type(row.iloc[5]) == int)
    price = (type(row.iloc[6]) == float)

    type_checks = [id, loc, title, bedroom, bathroom, parking_space, price]

    if False in type_checks:
        return False

    return True

# ingest data in db
def ingest_data(conn, df):
    """
    """
    
    columns = ", ".join([str(x) for x in df.columns.tolist()])

    logging.info("data ingestion starting...")
    with conn.cursor() as cursor:
        for i, v in df.iterrows():
            if validate_row(v) == True:
                sql = "INSERT INTO housing_dataset (%s) VALUES %s" % (columns, tuple(v))
                cursor.execute(sql)
                conn.commit()
            else:
                logging.error(f"invalid datatype on row {i}")
        logging.info("data ingestion completed")


if __name__=="__main__":

    load_dotenv()

    database = os.environ.get("DATABASE")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("HOST")
    url = os.environ.get("URL")
    sheet = os.environ.get("SHEET")

    conn = connect_db(database, db_user, db_password, host)
    create_table(conn)
    data = load_data(url, sheet)
    ingest_data(conn, data)
    conn.close()
