import os
from push_data import load_file_into_dataframe, add_email, create_worksheet
import pandas as pd
import gspread
import json
from unittest.mock import MagicMock
import pytest


def test_service_file_path(load_service_file):
    """
    This test the service file path declared in .env if it contains neccessary keys
    """
    _, keys = load_service_file
    assert 'private_key_id' in keys
    assert 'project_id' in keys
    assert 'client_id' in keys
    assert 'token_uri' in keys
    assert 'auth_uri' in keys

def test_load_file_into_dataframe():
    """
    tests the load dataframe function
    """

    file = "project-3/data/Housing_dataset_train.csv"
    df = load_file_into_dataframe(file)

    assert type(df) == pd.DataFrame
    assert df.isna().sum().max() == 0

def test_add_email():
    """
    tests the add email function
    """

    #using magicmock to mock gspread.spreadsheet
    spreadsheet = MagicMock(return_value=gspread.spreadsheet)
    email = "test@gmail.com"
    assert add_email(spreadsheet, email) is None
    
def test_create_worksheet():
    """
    tests create worksheet function
    """

    # using magicmock to mock gpread.spreadsheets
    sheet = MagicMock(return_value=gspread.spreadsheet)
    worksheet = create_worksheet(sheet, title="title")
    assert worksheet
    