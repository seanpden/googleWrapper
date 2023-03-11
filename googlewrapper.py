import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd

class GoogleSheetsWrapper:
    def __init__(self, spreadsheet_id: str) -> None:
        """A wrapping class for basic CRUD interaction with google's API - specifically for sheets
    
        Args:
            spreadsheet_id (str): the spreadsheet ID found in the URL of your google sheet

        Attr:
            self.SPREADSHEET_ID (str): see args.
            self.SCOPES (list[str]): the url to decide if read_only
            self.row_hei (int): the height of the spreadsheet data
            self.row_wid (int): the width of the spreadsheet data
        """
        self.SPREADSHEET_ID = spreadsheet_id
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.row_hei = None
        self.row_wid = None

        self.creds = None
        if os.path.exists('token.json'):
            self.creds = Credentials.from_authorized_user_file('token.json', self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', self.SCOPES)
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(self.creds.to_json())

        service = build('sheets', 'v4', credentials=self.creds)
        self.sheet = service.spreadsheets()

    def setrow_hei(self, row_hei) -> None:
        """Set self.row_hei

        Args:
            row_hei (int): height of the data
        """
        self.row_hei = row_hei

    def setrow_wid(self, row_wid: int) -> None:
        """set self.row_wid

        Args:
            row_wid (int): width of the data
        """
        self.row_wid = row_wid

    def create_data(self, values: list, valueInputOption: str="USER_ENTERED") -> dict:
        """Appends data to the sheet/creates a 'row'

        Args:
            values (list): the desired values to fill the columns
            valueInputOption (str, optional): How to interpret data, "RAW" or "USER_ENTERED". Defaults to "USER_ENTERED".

        Returns:
            dict: prompt from google's api
        """
        try: 
            values = [values]
            body = {
                'values' : values
            }

            result = self.sheet.values().update(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f"a{self.row_hei+1}",
                valueInputOption=valueInputOption,
                body=body).execute()
            self.read_data()
            return result
        except HttpError as err:
            return err

    def read_data(self, range: str='Sheet1!a:z') -> pd.DataFrame:
        """Reads the data from the sheet

        Args:
            range (str, optional): The range of the sheet to read data from. Defaults to 'Sheet1!a:z'.

        Returns:
            pd.DataFrame: Dataframe of the data in the sheet
        """
        try:
            result = self.sheet.values().get(spreadsheetId=self.SPREADSHEET_ID, range=range).execute()
            values = result.get('values', [])
            df = pd.DataFrame(values)
            self.setrow_hei(df.shape[0])
            self.setrow_wid(df.shape[1])
            return df
        except HttpError as err:
            return err

    def update_data(self, range: str, values: list, valueInputOption: str="USER_ENTERED") -> dict:
        """Updates the data in the range selected

        Args:
            range (str): range of cells to update
            values (list): values to update the cells with
            valueInputOption (str, optional): How to interpret data, "RAW" or "USER_ENTERED". Defaults to "USER_ENTERED".

        Returns:
            dict: google's api response
        """
        try: 
            values = [values]
            body = {
                'values' : values
            }

            result = self.sheet.values().update(
                spreadsheetId=self.SPREADSHEET_ID,
                range=range,
                valueInputOption=valueInputOption,
                body=body).execute()
            self.read_data()
            return result
        except HttpError as err:
            return err

    def delete_data(self, range: str) -> list:
        """Deletes a 'row' of values

        Args:
            range (str): the range to delete

        Returns:
            list: google's api response
        """
        try: 
            values = [[""]*self.row_wid]
            body = {
                'values' : values
            }

            result = self.sheet.values().update(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f"{range}",
                valueInputOption="USER_ENTERED",
                body=body).execute()
            self.read_data()
            return result
        except HttpError as err:
            return err