from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from typing import List, Union
import json
 
app = FastAPI()
 
# Google Sheets API configuration
SERVICE_ACCOUNT_FILE = '''{
  "type": "service_account",
  "project_id": "datapopulation-441708",
  "private_key_id": "7d179783c45feed6a793ef12a8aafa18bda2d953",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCi099bzOAbhmpw\n+UPIA2k4MC/EF3o88zLS2RFWWBC0RMpk59Bnq9DmzRcCuBM1+FfDOT05xDP9QnvN\nHnBEAydEK9BYQCFg+xKcbujETfBg3pjZOg/edRxTgw9nn5eS+2IurSlERGlhn4BH\nX5tOCv7AoM73If1QPqqFsTWA8xjiyrLvUHSr5krVrrnMPwXKlWYR9RTEPF7BNGf2\nZ/6rzsHanYl2aa3aJL8awmHLhOZCdVy7FbpCH4gU+Z69DcHvX9H45vUYvnUjRVJL\nB7EgksCG4DfiWwI976rfpQDJdWCZaoFAyp0RwZLc3WafVrACafFuxyJ0N9cH6qMT\nqu6zVaiXAgMBAAECggEAE1KpaForzkMT+/bYwF0HQ4+/Bg+d21XRFjcvA4bNP1da\niBb/zbhqFg8InV39iaNrEX6IHC7Yge7ohN2z3POy2Tg0hVnhnCyvd2OsuSm2o7Lx\ng0s3TuL+9Y98Ey6xQilq7AxzSpKudCiwLx19b17b/z1rfOXKYJd54lu2nxLk78+F\nd8d3O/x3eW7Jl2NxDvSusdIsO3yQf9F1xbXbE7h3oZuLSIq13r51DKJjKn+20hPJ\nsTzOkpqcYdFPEJqSPiO+n51W8ScGCoOrHapKfBWHdR8ZbLefnKS3nfmu1/99MaSY\nSU4N3HrAkbDnqIdvlC0dtKFuGYgFW1OWcwO02gs+gQKBgQDkMnuGV+km4QZcwFpW\nUa1GKB4QygU2TF7Mk2xHHHvmhSMGf22OKPvOdutnwdwVPpsOvQRj+A0Su8GGUDJM\nGVbu3zrPQ9e1Kbzz+s4x6XhPKQZwKG+88Oud/M5YIx4ilFDAmAdsED1PeAFzObnZ\n1AuwjBs3kws49IDzN9oOc3rkZwKBgQC2qn/odxC7e6vRoTX+3/ycKpcibNNE1udq\nySrTumSb5Bk52BwVMuH6w6IkURo5Xbzc/YvsUZT4sR2O0QtK49aFrvAAqgBzeIhn\nDBBTKTvx0/UgYvEkEZSQ0C/mhU3PmpGitXT3V1eBs17eakAvX/BiGGATmSC3a7qO\n8dIb97L8UQKBgQCQCq2eJ2+scu4pLeHYCsZtOnV/84VGuP498/CtEnXDFNb/CwCE\nc6o+sSH25I+feV02+GkFEVZvNMT7mzOkhXoyXgYpPpGycR4sg2wUZWLj1OmTcnLV\nuN2BZEp7p+8ltKkkaNKGi9mniACiUxXVQdv/HPExK0gyM2QSIncqGArCkQKBgE1t\nGchsZ5BAjITPw3ZkdMuxFdzGqgp82RyfZmLWl/yoA7VMECNALR9Au0KPigEU5Y/6\nfMvPjMiZMoEtxI6a4nxJuXtek9BeH9sl9ul5CeqoQDfZGIOEXCfBxvv8Nw4vY1nH\ndmu8/t+AK081VD2AegDqehb0ijSVGj+q6rdmF9zxAoGBALP25QX3rbbBZxEf5DpH\nYUt7aB8+28jt+zhCVu+U7REGOxbkX0seUGcJsRFu5bZ2rZ48vqd+jq9efEl9vhrX\nArw/WrfOQhHjswDcSPbf8+l6dMtsmGUI77hZdOvhmZGbbNvM1SWfyxfMY3wZ78s7\nIu+M48pcVo35KLv9viKA0/Sw\n-----END PRIVATE KEY-----\n",
  "client_email": "suryaprakash-vss@datapopulation-441708.iam.gserviceaccount.com",
  "client_id": "118265818436158453765",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/suryaprakash-vss%40datapopulation-441708.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}'''
SPREADSHEET_ID = '10mXRRhQ0MWphsSTdCKgBFOpQYIUZd5wwSCfHRxsc9Lo'
RANGE_NAME = 'sensor_data!A2:D'  # Adjusted to allow dynamic column range
 
 
credentials_info = json.loads(SERVICE_ACCOUNT_FILE)
# Google Sheets API credentials
credentials = service_account.Credentials.from_service_account_file(
    credentials_info,
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
 
