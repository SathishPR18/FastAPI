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

service_account = {
    "type": "service_account",
    "project_id": "datapopulation-441708",
    "private_key_id": "0c2d39045082d7703f987ef2096723d554d9a71e",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC4Hx+aOfho5lBy\nIkakL4gqbJrx+swyK5pX47PAdvn+7h69ty0/LOChCHQwBN3pTpz2jVkWuvbzI2MT\ny/qhSqjhtRRytOpdgJ8K0Z7kLcCrxBIAtfRXMcMhmd5qI7Nemp7IXrxw3DdlSZWP\ni6SNBxYlO48XMGhvPnhUdyk+eMnAnhyaX3LKnTeN7lHZBKtqSkOz0mq72IRDlC4g\nNu8rFtilkn0XuwDvxunoNlhM9XOZKE5W9cyi68eiaxx0YWHcUjbIynMLbwP5bBjg\nI0osnmtx6nG9y6C8DnCn3pQoDgIHUxK/PzDkhx34ESeyrwr3Na1BcqGR86ge7N5s\nMiG1GUtzAgMBAAECggEABMxJYInbSDwoOrEtPbhAGcUZqafldN/Ija855kqcJeFx\nNAAOjCfQgIqAw+BOC70chUm5Td/4TmG1glfUzFhLuOMZfX6lbB2e2NTIINhFeCjF\n27CznMoTXVA/iMn+d4TVbsuh7pQRbjZ03I97M4meGe+9BwY3Xh7CcinDsHjjgcMR\noj5XysQlZ7PjMrxwoPrXhWzQBlNF9Twa7NMHTk2CgkbOyXy0TI3Ebb8MxYXXCBxi\nTikszNhTLtXGGxBAdwnmdkHRM+aMXGjDV7o13vbkztkYZ9Aqcs98kFm6aQ8Sasf5\nIuIF6YWLHaM3b9FiWHD/QNMjDHS7G6LI/Rgmg8LxwQKBgQD0NqugEcK+c83OVeVB\nrrb+xoVGCRLJr/TSS55hnk9XAflScULuVF6DcO5dijFj3Y9jQrXxVshy6mB+Pdx5\nM5GT/dmGdhi2IE5Fi1I1awXDqF8thEXZumGV/Gy/42JZmnRYXLlNILz7Sk+nV0hR\nc4tAiD0Z32XPLymHhyDGeYAp0QKBgQDBAf/GkFzcAOLyiN+D2Przj1wwP0PD0kxK\ncGp5PJV0bOtlo5MWJv6de81B4/qfj7JIwJodfsV9dCvIJBGjnwy5HiJO56DHO08V\ng1o0DxMkrZh14ddrXgzGMujRCxP1ATYeK8I/dk4rQBbPWTvwKH1PXriBkl9cw+1F\nojSno9FuAwKBgFYD54EyoEAKc8OoF16CFiw4afqX+YkWM4naXoeNhe5kYJd8ExH0\nn3F2Vk6V/P1qrTVN6t19Lo7jJGdyjHQYCL9yWqp0cBG94TO3dZYhAt++Lv/OJfgh\nIHv+c6NGiH5821vAFPgofseXjeSn5m3h33s0QSkauTY3K05z9sRRSSXxAoGBAJDH\nuB8yIYYi4BsBE3Gq7SyQ6J2Eh4e4E+RvCV+iU5Y/MiFPH8GpRKvLt3/qzRCuURWD\n7NedRfXClCkQ7W5om7mtYh0AYbmxwrQnkR2mBT0pP0mGPPrcVQbEH6LYBYNvAjKF\nrrO+Qgrti27EyFJxfPZx3kPMEd7bVfw7HfZ2y14TAoGAUnhcayQa+ipDPxgdjQKX\n8/fPr+hB289UHhBsW3MyBLgBLArIMpOgtMXbZsaqAQYpyIUbzduPP0AEoARzJSk9\nnDWUXjqTXA49ovzxuNE9K7Rb+GvtjXBYztEl60jcYmzR8fqLF+6Csxv6Wyixu3ge\n/mjKveehig8SsXXBs4FzUHw=\n-----END PRIVATE KEY-----\n",
    "client_email": "surya-prakash@datapopulation-441708.iam.gserviceaccount.com",
    "client_id": "112047189843085462309",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/surya-prakash%40datapopulation-441708.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
  }

# Google Sheets API credentials
credentials = service_account.Credentials.from_service_account_file(
    service_account,
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
