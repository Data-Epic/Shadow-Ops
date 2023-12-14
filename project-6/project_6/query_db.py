# import libraries
import os
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session, declarative_base
from dotenv import load_dotenv
import logging

# import ORM class from main
from main import ArtWork

# load environment variables
load_dotenv()

username = os.environ.get("USER")
password = os.environ.get("PASSWORD")
host = os.environ.get("HOST")

# prepare connection to db
engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}")
Base = declarative_base()
Base.metadata.create_all(bind=engine)

# total records in database
with Session(bind=engine) as session:
    query = session.query(ArtWork).count()
    logging.info(f"number of records is : {query}")

# top 5 nationalities in database
with Session(bind=engine) as session:
    query = session.query(ArtWork.nationality, func.count(
        ArtWork.nationality)).group_by(ArtWork.nationality).order_by(
            func.count(ArtWork.nationality).desc())
    logging.info(f"Top 5 Nationalities : {query.all()[:5]}")

# group by counts for departments
with Session(bind=engine) as session:
    query = session.query(ArtWork.department, func.count(
        ArtWork.department)).group_by(ArtWork.department).order_by(
            func.count(ArtWork.department).desc())
    logging.info(f"Departments : {query.all()}")

# top 5 medium used in the database
with Session(bind=engine) as session:
    query = session.query(ArtWork.medium, func.count(ArtWork.medium)).group_by(
        ArtWork.medium).order_by(func.count(ArtWork.medium).desc())
    logging.info(f"Top 5 Medium used : {query.all()[:5]}")

# gender distribution in database
with Session(bind=engine) as session:
    query = session.query(ArtWork.gender, func.count(ArtWork.gender)).group_by(
        ArtWork.gender).order_by(func.count(ArtWork.gender).desc())
    logging.info(f"Gender Distribution : {query.all()}")
