import os
import pytest
import json
from dotenv import load_dotenv

@pytest.fixture
def load_service_file():
    """
    This functions handles loading service file
    """
    load_dotenv()
    path = os.getenv("service_file_path")

    with open(path) as jsonFile:
        data = json.load(jsonFile)
        keys = data.keys()
    
    return data, keys