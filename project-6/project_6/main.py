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
                    {self.date_acquired_year} {self.department} {self.display_name} \
                        {self.end_date} {self.gender} {self.medium} {self.nationality} \
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
        logging.debug(f"please verify path: {path}")
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
        "constituent_id": int,
        "display_name": str,
        "nationality": str,
        "gender": str,
        "begin_date": int,
        "end_date": int,
        "title": str,
        "medium": str,
        "accession_number": str,
        "classification": str,
        "department": str,
        "date_acquired": datetime.datetime,
        "object_id": float,
        "completed_date": float,
        "date_acquired_year": int,
        "date_acquired_month": int,
        "date_acquired_weekday": int,
    }

    # check for datatypes of input against the validation dictionary
    for idx in dict_row_type.keys():
        if isinstance(row[idx], dict_row_type[idx]) is False:
            logging.error(f"Type check failed for :{idx}, {row[idx]}")
            return False
    return True

def rename_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns in dataframe into required format

    Parameters
    ----------
        data: pd.DataFrame
            pandas dataframe containing data

    Returns
    -------
        pd.DataFrame
    """
    column_dict = {
        "ConstituentID": "constituent_id",
        "DisplayName": "display_name",
        "Nationality": "nationality",
        "Gender": "gender",
        "BeginDate": "begin_date",
        "EndDate": "end_date",
        "Title": "title",
        "Medium": "medium",
        "AccessionNumber": "accession_number",
        "Classification": "classification",
        "Department": "department",
        "DateAcquired": "date_acquired",
        "ObjectID": "object_id",
        "completedDate": "completed_date",
        "DateAcquired_year": "date_acquired_year",
        "DateAcquired_month": "date_acquired_month",
        "DateAcquired_weekday": "date_acquired_weekday",
    }
    data = data.rename(columns=column_dict)

    return data

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

    logging.info("Data ingestion into database started")
    for idx in data.iterrows():
        try:
            row = idx[1].to_dict()

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
    db_uri = os.environ.get("DB_URI")
    path = os.environ.get("FILE_PATH")

    # create connection to postgres
    engine = create_engine(db_uri)
    # create table in db
    Base.metadata.create_all(bind=engine)

    with Session(bind=engine) as session:
        logging.info("Opening database session")
        data = load_data_from_file(path)
        data = rename_columns(data)
        ingest_data(data)

    logging.info("Closing all session")
