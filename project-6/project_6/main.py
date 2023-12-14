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
class ArtWork(Base):
    """
    A class to create an Artwork table using ORM classes
    """

    __tablename__ = "artwork"
    __table_args__ = {"extend_existing": True}

    constituent_id = Column(Integer, primary_key=True, index=True)
    display_name = Column(String, nullable=False)
    nationality = Column(String)
    gender = Column(String)
    begin_date = Column(Integer)
    end_date = Column(Integer)
    title = Column(String)
    medium = Column(String)
    accession_number = Column(String)
    classification = Column(String)
    department = Column(String)
    date_acquired = Column(DateTime)
    object_id = Column(Integer, primary_key=True, index=True)
    completed_date = Column(Integer)
    date_acquired_year = Column(Integer)
    date_acquired_month = Column(Integer)
    date_acquired_weekday = Column(Integer)

    def __repr__(self):
        return f"({self.accession_number} {self.begin_date} \
            {self.classification} {self.constituent_id} {self.date_acquired} \
                {self.date_acquired_month} {self.date_acquired_weekday} \
                    {self.date_acquired_year} {self.department} {self.DisplayName} \
                        {self.end_date} {self.gender} {self.medium} {self.Nationality} \
                            {self.object_id} {self.title} {self.completed_date})"


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
        print(path)
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
            logging.error(f"Type check failed for :{idx}, {row[idx]}")
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

            logging.info("Data ingestion into database started")
            # validate the row before inserting into db
            if type_check(row):
                line = ArtWork()
                for key, value in row.items():
                    setattr(line, key, value)
                session.add(line)
                session.commit()
            else:
                logging.debug(f"Type check failed for {idx[0]}:{row}")
        except IntegrityError as e:
            logging.error(e)
            session.rollback()

    logging.info("Data ingestion completed")


if __name__ == "__main__":
    # load environment variables
    username = os.environ.get("USERNAME")
    password = os.environ.get("PASSWORD")
    host = os.environ.get("HOST")
    path = os.environ.get("PATH")

    # create connection to postgres
    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}")
    # create table in db
    Base.metadata.create_all(bind=engine)

    with Session(bind=engine) as session:
        logging.info("Opening database session")
        data_path = path
        data = load_data_from_file(data_path)
        ingest_data(data)

    logging.info("Closing all session")
