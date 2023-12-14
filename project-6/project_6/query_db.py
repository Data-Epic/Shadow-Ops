# import libraries
import os
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session, declarative_base
from dotenv import load_dotenv
import logging

# import ORM class from main
from main import Artwork

# load environment variables
load_dotenv()

username = os.environ.get("username")
password = os.environ.get("password")
host = os.environ.get("host")

# prepare connection to db
engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}")
Base = declarative_base()
Base.metadata.create_all(bind=engine)

# total records in database
with Session(bind=engine) as session:
    query = session.query(Artwork).count()
    logging.info(f"number of records is : {query}")

# top 5 nationalities in database
with Session(bind=engine) as session:
    query = session.query(Artwork.Nationality, func.count(
        Artwork.Nationality)).group_by(Artwork.Nationality).order_by(
            func.count(Artwork.Nationality).desc())
    logging.info(f"Top 5 Nationality : {query.all()[:5]}")

# group by counts for departments
with Session(bind=engine) as session:
    query = session.query(Artwork.Department, func.count(
        Artwork.Department)).group_by(Artwork.Department).order_by(
            func.count(Artwork.Department).desc())
    logging.info(f"Departments : {query.all()}")

# top 5 medium used in the database
with Session(bind=engine) as session:
    query = session.query(Artwork.Medium, func.count(Artwork.Medium)).group_by(
        Artwork.Medium).order_by(func.count(Artwork.Medium).desc())
    logging.info(f"Top 5 Medium used : {query.all()[:5]}")

# gender distribution in database
with Session(bind=engine) as session:
    query = session.query(Artwork.Gender, func.count(Artwork.Gender)).group_by(
        Artwork.Gender).order_by(func.count(Artwork.Gender).desc())
    logging.info(f"Gender Distribution : {query.all()}")
