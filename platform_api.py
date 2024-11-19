from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from typing import List, Union
import json
from google.oauth2 import service_account
import json
app = FastAPI()

# Google Sheets API configuration
SERVICE_ACCOUNT_FILE = 'D:/FastAPI/gsheetcreds.json'
SPREADSHEET_ID = '10mXRRhQ0MWphsSTdCKgBFOpQYIUZd5wwSCfHRxsc9Lo'
RANGE_NAME = 'sensor_data!A2:D'  # Adjusted to allow dynamic column range
from google.oauth2 import service_account
import json

# Assuming `json_string` contains your JSON credentials as a string
json_string = """
{
  "type": "service_account",
  "project_id": "sensordata-442115",
  "private_key_id": "df896e6f8fb74ee6afbe43b1e51c3cdcd42f1b05",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCX/OSsF8IHYT04\n6YBFigwurU9hfdt3FiGZ6ikT2zQVnc8bGZF+irf+LsigZ2nW8Nqudqqy2Lt0W6G4\nJG1kXpDCxN4QI4BGRVkvLWi7z20oKJbnjRU+3nW4/cSN5X8S4syuaqkx1DUQtfBG\nqK5r/wlbtaRmPL8fOW2Nhh0YOsBXxu9Ow5TK2XFVe+gtItasGQe16qoxloQ8Jf6g\n7G67OCiuDJeC4IO/uk/qORnROm1d+5FN+vGSoLlMaEtRdBvKo5zcD+CNST7DzQSc\nmPXmcNgc8kR6GLstjhYzWeqkAkD7pjJHzgN2kLCdvdPABBMBpFxYYZvuc8n3Klyh\njIYzjnajAgMBAAECggEAAZItLfP5qCdxV0oK7q7oT7NClwH5KHbkHMtTcZJI41L/\ngT/R2eiVTghJVfG+5zFyvj6KxAmpGMWs14ecokGECR1LkbAxgpGUgklJX39CFYtd\n+ccAE/J/QfUZjNOWGRzc1S7tqEzeauvYHYw65Nvv9n9UHLYF5AABSp3fkW4EqeSB\niljUAuLxYXMSWLOGHvEFYgTptG2eac2Qy4530C4DC59XCVCCYSXOvql4wpGnCjJL\nf7PrhfzLlXFY5wfRKHa5hu+mMB2Z6S2Yj2OM1Y32RDPZdeKBgCyHO56v7JpwIIkV\nCSWoRDYOOYq9gs1ipFVlNxkgf9rM9pOZHO1HxMB6/QKBgQDH8O15V1dQU84XXMle\nDxFc/rBSff6ZPO68DMG+u2EuCodq/hHyUczsYqcoYeXTv8/5YPPPHKOBvW2e7tKf\nOTf3cvacFtB2Tpq8GhXp1vqx2hJ3AlrXEfGFbJcMbdUzrhL0b1i9TLte5NDY3TOj\ne7Adunx6/aWW/L+VMYh1bpU/lwKBgQDCmgyFJXt5VlUDqdi0Su1cVVMN18WYwYnc\nTwHj7AnpWDg0TJ3LFPvI1iRCtnxY5Amkl5CmkxYcQ74EsGb4HmLzyuY8Q8K9hyOZ\nv96SsN9pD8v10htWPlt7W5yFTereblCNQSQh15vEOrOavbMey4jMPie0w3C5m7kC\nhwhwrlWi1QKBgD3GogkAxHiMPDwZrhoCu9Go25/RUA3WtsihhGdDDAamuCqFr9PY\nFGHhJVaj0Nf5BvA9VXdjmN1oQut9TNRnYHRzL+EQZ352UPbXdHfYtYKoJ1ZgAuM6\npw4bfBwZ/2rFWRPvJ1Lt12K9fg3TYrYbbFHzIaz6m+Qn2aXmNQxbi3+nAoGAGA06\nJuqvDBwjfcRsSDxKgfL5xOg6P9sL0vLg5O6zeuewaqZdilAZnoT5DlXHoKTunC0v\nb6cWQIAO6D11PI1k5YTaV/B4DTy6pbIVLEQ4GKAfMU66mjoyzFhSTBeJQ9yWkB7Q\n/G3Njr9Cm4l+rfux+Kxl8+2D7SzV/P37iRHC+9ECgYEAiwRc0JXFjH9c8zXlS1tu\nDCEuYn5/wN6z42WOqFLb4FZHDdt0nIawt5UvqnNqqW0aV1YQYYS4WqFbqPAlQjIV\n4cICpLqNIDyZsaRFT7alVPi3yss/PQJ+4UrC98FvL3q1ezfZlJqOPWhN7rSYOhCh\n+NtpysCPKezVqyGdIMra/kY=\n-----END PRIVATE KEY-----\n",
  "client_email": "testsensordata@sensordata-442115.iam.gserviceaccount.com",
  "client_id": "104584075051512829103",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testsensordata%40sensordata-442115.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
"""

# Parse the string into a dictionary
service_account_info = json.loads(json_string)

# Use from_service_account_info for dictionary-based credentials
credentials = service_account.Credentials.from_service_account_info(service_account_info)

# Verify credentials object
print(credentials)

# Parse the JSON string into a dictionary
service_account_api = json.loads(json_string)

# Create credentials object from the dictionary
credentials = service_account.Credentials.from_service_account_info(
    service_account_api,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

# Initialize the Google Sheets API service
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()


# Define the data model for the incoming JSON structure
class SensorDataIncoming(BaseModel):
    cmd: str
    seqno: int = None
    EUI: str
    ts: int = None
    fcnt: int = None
    data: str
    rssi: float = None
    snr: float = None
    protocol: str

# Define the data model for Google Sheets (subset of SensorDataIncoming)
class SensorData(BaseModel):
    EUI: str
    ts: int
    data: str

@app.post("/Sensor-data/")
async def post_data(data: Union[List[SensorDataIncoming], SensorDataIncoming]):
    try:
        # Convert single object input to a list
        if isinstance(data, SensorDataIncoming):
            data = [data]
        
        # Prepare data to insert into Google Sheets
        values = []
        for entry in data:
            # Convert ts to a readable timestamp in UTC
            timestamp = datetime.utcfromtimestamp(entry.ts / 1000).strftime("%m-%d-%Y %H:%M:%S") if entry.ts else ""
            sensor_data = SensorData(EUI=entry.EUI, ts=entry.ts or 0, data=entry.data)
            values.append([sensor_data.EUI, sensor_data.ts, sensor_data.data, timestamp])

        # Prepare body for API request
        body = {'values': values}

        # Append data to Google Sheets
        sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption="RAW",
            body=body
        ).execute()

        return {"status": "Data successfully added to Google Sheet", "code": 201}
    except Exception as e:
        # Raise a 500 error with detailed information
        raise HTTPException(status_code=500, detail=f"Failed to add data: {str(e)}")
