# GSPREAD Project

This project is about populating a google spreadsheet using python library `gspread`.

The dataset source for this project is a Nigerian Housing Dataset hosted [here](https://github.com/GbotemiB/MLOps_zoomcamp/raw/main/data/Housing_dataset_train.csv). 

## Getting Started
To get started with this project,
you will need to create a virtual environment and install the following libraries using the command below.

- To create a virtual environment with conda, run 
    ```
    conda create --name gspread-project
    ```
- To activate the created environment, run 
    ```
    conda activate gspread-project
    ```
- To install the needed libraries run 
    ```
    pip install gspread pandas python-dotenv
    ```
- change working directory
    ```
    cd gspread-project
    ```

### Enable API access

- Head to [Google Developers Console](https://console.developers.google.com/) and create a new project (or select the one you already have).
- In the box labeled “Search for APIs and Services”, search for “Google Drive API” and enable it.
- Go to “APIs & Services > Credentials” and choose “Create credentials > Service account key”.
- Fill out the form
- Click “Create” and “Done”.
- Press “Manage service accounts” above Service Accounts.
- Press on ⋮ near recently created service account and select “Manage keys” and then click on “ADD KEY > Create new key”.
- Select JSON key type and press “Create”.
- This will automatically download a json file. 
- Run `touch secret.json` in the project directory to create a new file. 
- Store the content of the downloaded json file in the created file.

### Environment Variables
create an `.env` file to store all the variables needed to run this script successfully.

Run `touch .env` to create the file.
Copy the following into the file and replace with your details.
```
email = <replace with email>
service_file_path = <replace with file_path to secret.json>
sheet_title = <replace with sheet_title>
worksheet_title = <replace with worksheet_title>
file_path = <replace with file path>
```

## Workflow

`push_data.py` is the main script. This script takes as input;
- **user_email**: user_email will be added as an editor with a link to the created file in gmail
- **file_path**: file_path is the path to the dataset or direct link to the file
- **service_file_path**: this is the path to the secrets for google api downloaded
- **sheet_title**: this is the title of the googlesheet
- **worksheet_title**: this is the worksheet title

The main script creates a new google spreadsheet and user_email as an editor. It then load the dataset into a pandas dataframe. The dataframe is thereafter used to fill the google worksheet.

To run the script, run
    ```
    python push_data.py
    ```

## Contribution to Knowledge
To learn more about gspread package, [click here](https://docs.gspread.org/en/v5.10.0/index.html)