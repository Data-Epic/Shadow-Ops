import os
from dotenv import load_dotenv
import gspread
import pandas as pd
import logging
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO)

def service_file(file_path: str) -> gspread.client:
    """
    this function load google service account file
    """
    gc = gspread.service_account(file_path)
    logging.info("service file loaded successfully")

    return gc

def create_spreadsheet(gc: gspread.client, sheet_title: str) -> gspread.spreadsheet:
    """
    this function create a new spreadsheet
    """
    sheet = gc.create(sheet_title)
    logging.info("google sheet created succesfully")

    return sheet

def add_email(sheet: gspread.spreadsheet, user_mail: str) -> None:
    """
    this function add user_email as a writer to the spreadsheet
    """
    sheet.share(user_mail, perm_type='user', role='writer')
    logging.info("email added successfully")

    return None

def create_worksheet(sheet: gspread.spreadsheet, title: str) -> gspread.worksheet:
    """
    function creates a worksheet
    """
    worksheet = sheet.add_worksheet(title="first", rows=100, cols=20)
    logging.info("worksheet successfully created")

    return worksheet

def load_file_into_dataframe(file: str) -> pd.DataFrame:
    """
    this function read the dataset with pandas and drop nan values
    """
    df = pd.read_csv(file)
    df.dropna(inplace=True)
    logging.info("files successfully loaded into dataframe")

    return df

def update_worksheet(worksheet: gspread.spreadsheet, df: pd.DataFrame) -> None:
    """
    this function update the created worksheet:worksheet with data from dataframe:df
    """
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    logging.info("worksheet updated successfully")

    return None

def run(user_email: str, file: str, service_file_path: str, sheet_title: str, worksheet_title: str) -> None:
    """
    this function runs the whole process of loading a dataset into a google spreadsheet
    """
    gc = service_file(service_file_path)
    sheet = create_spreadsheet(gc, sheet_title)
    add_email(sheet, user_email)
    worksheet = create_worksheet(sheet, worksheet_title)
    df = load_file_into_dataframe(file)
    update_worksheet(worksheet, df)

if __name__=="__main__":
    # load environment variable
    load_dotenv()

    my_password = os.getenv("Password")
    # the user_email is needed as it will grant the user access to the google sheet and grant the user permission to edit the worksheet
    user_email = os.getenv("email")

    # file is the filepath to the dataset on local system or a direct link
    file = os.getenv("file_path")

    # service_file_path is the filepath to the secret to access google apis
    service_file_path = os.getenv("service_file_path")

    # sheet_title is the title of the google spreadsheet
    sheet_title = os.getenv("sheet_title")

    # worksheet_title assign a title to the worksheet
    worksheet_title = os.getenv("worksheet_title")

    # the run method execute the whole process
    run(user_email, file, service_file_path, sheet_title, worksheet_title)