import os
import pytest
import pandas as pd
from project_6.main import load_data_from_file, type_check, ingest_data
from unittest.mock import patch, Mock


def test_load_data_from_file():
    # assert if the result is a pd.dataframe
    pass


def test_type_check():
    test_dict = {
        'ConstituentID': 135298,
        'DisplayName': 'William Theophilius Brown',
        'Nationality': 'American',
        'Gender': 'Male',
        'BeginDate': 1919,
        'EndDate': 2012,
        'Wiki QID': 'Unknown',
        'ULAN': 0.0,
        'Title': 'The Room',
        'Medium': 'Lithograph',
        'Dimensions':
        'composition (irreg.): 8 1/2 × 10 11/16" (21.6 × 27.2 cm); sheet: 15 3/16 × 17 7/8" (38.5 × 45.4 cm)',
        'CreditLine': 'Gift of Kleiner, Bell & Co.',
        'AccessionNumber': '707.1967',
        'Classification': 'Print',
        'Department': 'Drawings & Prints',
        'DateAcquired': pd.Timestamp('1967-12-13 00:00:00'),
        'Cataloged': 'Y',
        'ObjectID': 73335.0,
        'URL': 'http://www.moma.org/collection/works/73335',
        'ThumbnailURL':
        'http://www.moma.org/media/W1siZiIsIjI3NjYwMCJdLFsicCIsImNvbnZlcnQiLCItcmVzaXplIDMwMHgzMDBcdTAwM2UiXV0.jpg?sha=45e2b2c609fc62b3',
        'Height (cm)': 21.6,
        'Width (cm)': 27.2,
        'completedDate': 1960.0,
        'DateAcquired_year': 1967,
        'DateAcquired_month': 12,
        'DateAcquired_day': 13,
        'DateAcquired_weekday': 2
    }

    assert (type_check(test_dict))
    assert isinstance(type_check(test_dict), bool)


def test_ingest_data():
    # create mock dataframe
    # check if the dataframe is empty
    # check for the columns of the dataframe
    pass

