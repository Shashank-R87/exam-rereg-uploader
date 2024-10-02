from fastapi import FastAPI
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
from os import system, name, environ
from supabase import create_client, Client
import datetime

load_dotenv()
url: str = environ.get("SUPABASE_URL")
key: str = environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

KEY_FILE = environ.get("SHEETS_KEY_FILE")
SHEET_ID = environ.get("SHEETS_SHEET_ID")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]   
credentials = service_account.Credentials.from_service_account_file(
    KEY_FILE, scopes=SCOPES
)

service = build("sheets", "v4", credentials=credentials)

listIndex = {"sup": "Supplementary", "ads": "Additional Slot", "rr" : "Re-Registration", 'cc': "Contact Course"}
submissionIndex = {"sup": "Supplementary Submissions", "ads": "Additional Slot Submissions", "rr" : "Re-Registration Submissions", 'cc': "Contact Course Submissions"}
submissionSheetID = {"sup": 1830742708, "ads": 1354391646, "rr" : 753582134, 'cc': 775152585}

price = {"sup":[1500, 700], "ads":[2500, 2500], "rr": [2500, 0], "cc": [4000, 4000]}


def get_termout_price(reg_number, type):
    current_year = datetime.date.today()
    year = current_year.year % 100
    reg_year = int(reg_number[3:5])

    if (abs(year-reg_year)>4):
        return True, price[type][0]
    else:
        return False, price[type][1]

def getData(type):
    result = (service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=listIndex[type]).execute())["values"]
    result.pop(0)
    total_count = len(result)
    return total_count, result

def uploader(total_count, data, type, batch_size=1000):
    batch = []
    for i in range(total_count):
        term_out, price = get_termout_price(data[i][0], type)
        row_data = {
            "reg_number": data[i][0],
            "student_name": data[i][2],
            "reg_type": listIndex[type],
            "course_code": data[i][1].split(" ", 1)[0],
            "course_name": data[i][1].split(" ", 1)[1],
            "row_id": data[i][1].split(" ", 1)[0]+data[i][0]+listIndex[type],
            "term_out": term_out,
            "price": price
        }
        
        batch.append(row_data)
        
        if len(batch) >= batch_size:
            response = supabase.table("students_data").upsert(batch, on_conflict="row_id").execute()
            batch = []
        print(f"Uploading row \033[32m{i+1}\033[33m/\033[34m{total_count}\033[33m", end='\r')

    if batch:
        response = supabase.table("students_data").upsert(batch, on_conflict="row_id").execute()
        
def initialize_uploader():
    print("\n\033[7m Initializing Uploader Function... \033[0m")
    response = supabase.table("students_data").delete().neq("reg_number", -1).execute()
    if response.count == None:
        print("\n\033[7m Initialization Complete \033[0m")
    else:
        print("\n\033[91m Error: Unable to delete existing data \033[0m")

def clear():
    if name == 'nt':
        _ = system('cls')
    else:
        _ = system('clear')

def main_uploader():
    clear()
    # initialize_uploader()
    print("\n\033[7m Starting Uploader Function... \033[0m")

    for i in listIndex:
        total_count, data = getData(i)
        print(f"\nData Type: \033[0m\033[93m{listIndex[i]}\033[0m")
        print(f"Total rows found: \033[34m {total_count} \033[0m")

        if total_count!=0:
            print(f"\033[94mStarting\033[0m Upload of \033[93m{listIndex[i]}\033[0m data...")
            uploader(total_count, data, i)
            print(f"\033[92mCompleted\033[0m Upload of \033[93m{listIndex[i]}\033[0m data")
        else:
            print(f"\033[91mSkipping\033[0m upload of \033[93m{listIndex[i]}\033[0m data...")

    print("\n\033[7m Completed Uploading \033[0m\n")

app = FastAPI()

@app.get("/")
def read_root():
    main_uploader()
    return {"message": "Uploaded"}