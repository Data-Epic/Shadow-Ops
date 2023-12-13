# import libraries
import os
from sqlalchemy import create_engine, ForeignKey, Column, String, Integer, Date, DateTime, Float
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.ext.indexable import index_property
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
import pandas as pd
from dotenv import load_dotenv
import numpy as np
import logging
import datetime

# create orm sql classes
class Artwork(Base):
    """
    A class to create an Artwork table using ORM classes
    """
    __tablename__ = "artwork"
    __table_args__ = {'extend_existing': True}

    ConstituentID = Column(Integer, primary_key=True)
    DisplayName = Column(String, nullable=False)
    Nationality = Column(String)
    Gender = Column(String)
    BeginDate = Column(Integer)
    EndDate = Column(Integer)
    Title = Column(String)
    Medium = Column(String)
    AccessionNumber = Column(String)
    Classification = Column(String)
    Department = Column(String)
    DateAcquired = Column(DateTime)
    ObjectID = Column(Integer, primary_key=True)
    completedDate = Column(Integer)
    DateAcquired_year = Column(Integer)
    DateAcquired_month = Column(Integer)
    DateAcquired_weekday = Column(Integer)


# load data into dataframe
def load_data_from_file(path: str) -> pd.DataFrame:
    """
    Loads data from parquet file into pandas dataframe

    Parameters
    ----------
        path : str
            filepath to the parquet file
    
    Returns
    -------
        Pandas DataFrame
    """
    try:
        data = pd.read_parquet(path)
    except:
        raise SyntaxError("unable to load data from path")

    return data

# data validation
def type_check(row: dict) -> bool:
    """
    Validates data by checking the data type.
    If data type matches, then it returns True else False

    Parameters
    ----------
        row : dict
            variable containing values of a row
    
    Returns
    -------
        Boolean
    """

    dict_row_type = {
        'ConstituentID': int,
        'DisplayName': str,
        'Nationality': str,
        'Gender': str,
        'BeginDate': int,
        'EndDate': int,
        'Wiki QID': str,
        'ULAN': float,
        'Title': str,
        'Medium': str,
        'Dimensions': str,
        'CreditLine': str,
        'AccessionNumber': str,
        'Classification': str,
        'Department': str,
        'DateAcquired': datetime.datetime,
        'Cataloged': str,
        'ObjectID': float,
        'URL': str,
        'ThumbnailURL': str,
        'Height (cm)': float,
        'Width (cm)': float,
        'completedDate': float,
        'DateAcquired_year': int,
        'DateAcquired_month': int,
        'DateAcquired_day': int,
        'DateAcquired_weekday': int
    }

    for idx in row.keys():
        if isinstance(row[idx], dict_row_type[idx]) is False:
            print(idx, row[idx])
            return False
    return True

# ingest data
def ingest_data(data: pd.DataFrame) -> None:
    """
    Ingest data into postgres database from pandas dataframe

    Parameters
    ----------
        data : pd.DataFrame
            data to be ingested into dataframe
    
    Returns
    -------
        None
    """
    for idx in data.iterrows():
        try:
            row = idx[1].to_dict()
            if type_check(row, dict_row_type):
                line = Artwork()
                for key, value in row.items():
                    setattr(line, key, value)
                session.add(line)
                session.commit()
                logging.info(f"successfully logged {idx[0]}")
            else:
                logging.error(f"type check failed for {idx[0]}:{row}")
        except IntegrityError as e:
            logging.error(e)
            session.rollback()

    logging.info("data ingestion completed")


if __name__ == "__main__":
    load_dotenv()
    Base = declarative_base()

    username = os.environ.get("username")
    password = os.environ.get("password")
    host = os.environ.get("host")
    path = os.environ.get("path")

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}")

    Base.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    data_path = path
    data = load_data_from_file(data_path)
    ingest_data(data)

    session.close_all()
