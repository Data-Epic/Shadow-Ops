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
def connect_db(DATABASE: str, DB_USER: str, DB_PASSWORD: str, DB_HOST: str) -> sql.connection:
    """
    functions creates a connection to the database
    """

    try:
        connection = sql.connect(
            database=DATABASE,
            user=DB_USER,
            host=DB_HOST,
            password=DB_PASSWORD,
        )
        logging.info("opening connection to database")
    except:
        raise Exception("Cant connect to database")
        
    return connection

# create table
def create_table(connection: sql.connection) -> None:
    """
    functions creates a table in the specified database if doesnt exist
    """
    # query to create table in the database
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

    # execute query to create table
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)

            connection.commit()
            logging.info("table created successfully")
    except:
        logging.info("table already exists")
    
    return None

# load data into dataframe
def load_data(url: str, sheet_title: str) -> pd.DataFrame:
    """
    functions loads data from google sheets into pandas dataframe
    """
    # using gspread to open the data by url
    gc = gspread.service_account()
    sheet = gc.open_by_url(url)
    logging.info("opened url successfully")

    # load the data into a dataframe
    worksheet = sheet.worksheet(sheet_title)
    data = pd.DataFrame(worksheet.get_all_records())
    logging.info("data loaded into dataframe successfully")

    return data

# validate datatype
def validate_row(row: pd.Series) -> bool:
    """
    functions validates the type for each row
    Its check for each type in the row, if its contains a False then the type_checks fails
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
def ingest_data(conn: sql.connection, data: pd.DataFrame) -> None:
    """
    functions ingest data from dataframe into database.
    Its also runs the validation function for each row before ingesting it.
    """
    # columns to use in loading the data
    columns = ", ".join([str(x) for x in data.columns.tolist()])

    logging.info("data ingestion starting...")
    with conn.cursor() as cursor:
        for i, v in data.iterrows():
            if validate_row(v) == True:
                sql = "INSERT INTO housing_dataset (%s) VALUES %s" % (columns, tuple(v))
                cursor.execute(sql)
                conn.commit()
            else:
                logging.error(f"invalid datatype on row {i}")
        logging.info("data ingestion completed")

        return None

def run():

    load_dotenv()

    # loading environment variables
    database = os.environ.get("DATABASE")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOST")
    url = os.environ.get("URL")
    sheet = os.environ.get("SHEET")

    conn = connect_db(database, db_user, db_password, host)
    create_table(conn)
    data = load_data(url, sheet)
    ingest_data(conn, data)
    conn.close()
    logging.info("database connection closed")

if __name__=="__main__":
    run()

