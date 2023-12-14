# import libraries
import os
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    DateTime,
    Float,
)
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.exc import IntegrityError
import pandas as pd
from dotenv import load_dotenv
import logging
import datetime
import warnings

warnings.filterwarnings("ignore")

load_dotenv()
logging.basicConfig(level=logging.INFO)

Base = declarative_base()


# create orm sql classes
class Artwork(Base):
    """
    A class to create an Artwork table using ORM classes
    """

    __tablename__ = "artwork"
    __table_args__ = {"extend_existing": True}

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

    def __repr__(self):
        return f"({self.AccessionNumber} {self.BeginDate} \
            {self.Classification} {self.ConstituentID} {self.DateAcquired} \
                {self.DateAcquired_month} {self.DateAcquired_weekday} \
                    {self.DateAcquired_year} {self.Department} {self.DisplayName} \
                        {self.EndDate} {self.Gender} {self.Medium} {self.Nationality} \
                            {self.ObjectID} {self.Title} {self.completedDate})"


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

    logging.info("Data loaded into dataframe successfully")

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

    # dictionary containing data types that will be used for validation
    dict_row_type = {
        "ConstituentID": int,
        "DisplayName": str,
        "Nationality": str,
        "Gender": str,
        "BeginDate": int,
        "EndDate": int,
        "Wiki QID": str,
        "ULAN": float,
        "Title": str,
        "Medium": str,
        "Dimensions": str,
        "CreditLine": str,
        "AccessionNumber": str,
        "Classification": str,
        "Department": str,
        "DateAcquired": datetime.datetime,
        "Cataloged": str,
        "ObjectID": float,
        "URL": str,
        "ThumbnailURL": str,
        "Height (cm)": float,
        "Width (cm)": float,
        "completedDate": float,
        "DateAcquired_year": int,
        "DateAcquired_month": int,
        "DateAcquired_day": int,
        "DateAcquired_weekday": int,
    }

    # check for datatypes of input against the validation dictionary
    for idx in row.keys():
        if isinstance(row[idx], dict_row_type[idx]) is False:
            logging.error(f"type check failed for :{idx}, {row[idx]}")
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

    # iterate through the data to insert the values into the database
    for idx in data.iterrows():
        try:
            row = idx[1].to_dict()

            # validate the row before inserting into db
            if type_check(row):
                line = Artwork()
                for key, value in row.items():
                    setattr(line, key, value)
                session.add(line)
                session.commit()
            else:
                logging.error(f"type check failed for {idx[0]}:{row}")
        except IntegrityError as e:
            logging.error(e)
            session.rollback()

    logging.info("data ingestion completed")


if __name__ == "__main__":
    # load environment variables
    username = os.environ.get("username")
    password = os.environ.get("password")
    host = os.environ.get("host")
    path = os.environ.get("path")

    # create connection to postgres
    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}")
    # create table in db
    Base.metadata.create_all(bind=engine)

    logging.info("opening database session")
    with Session(bind=engine) as session:
        data_path = path
        data = load_data_from_file(data_path)
        ingest_data(data)

    logging.info("closing all session")
