from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from typing import List, Union

app = FastAPI()

# Google Sheets API configuration
SERVICE_ACCOUNT_FILE = 'D:/FastAPI/gsheetcreds.json'
SPREADSHEET_ID = '10mXRRhQ0MWphsSTdCKgBFOpQYIUZd5wwSCfHRxsc9Lo'
RANGE_NAME = 'sensor_data!A2:D'  # Adjusted to allow dynamic column range

# Google Sheets API credentials
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

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
