import os
import subprocess
from zipfile import ZipFile
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

path_google_play_console_sale_reports = r"gs://pubsite_prod_8737102155398054550/sales"
path_savefile_in_bucket = r"gs://voliovn_app/phucdee"
path_zipfile_in_bucket = r"gs://voliovn_app/phucdee/sales"
path_csvfile_in_bucket = r"gs://voliovn_app/phucdee"
path_zipfile_in_local = r"G:\Google Play Console Sale Reports\zipfile"
path_csvfile_inlocal = r"G:\Google Play Console Sale Reports\unzipfile\csv_sales"
file_path_to_check = r"G:\Google Play Console Sale Reports\zipfile\sales\salesreport_202008.zip"
start_date = '2020-08-01'


def create_command(path_from: str, path_to: str)-> str:
    return fr'gsutil -m cp -r "{path_from}" "{path_to}"'

def copy_file_from_GPC(path_from, path_to):
    command = create_command(path_from, path_to)
    subprocess.call(command, shell = True)


def download_file_from_bucket(path_from, path_to):
    command = create_command(path_from, path_to)
    subprocess.call(command,shell = True)
    # os.system('cmd /c gsutil -m cp -r  gs://voliovn_app/sales "G:\Google Play Console Sale Reports\zipfile"')

def check_file_download_succeed(file_path)->bool:
    modified_date = datetime.fromtimestamp(os.stat(file_path).st_mtime)
    modified_date_str:str = datetime.strftime(modified_date,"%Y-%m-%d")
    today_str:str = datetime.strftime(date.today(),"%Y-%m-%d")
    if today_str == modified_date_str:
        pass
    else:
        raise Exception("Error in Download")

def generate_list_date(start_date: str)-> list[dict]:
    list_date = []
    sdate = datetime.strptime(start_date, '%Y-%m-%d')
    today = datetime.today()
    while sdate <= today:
        if sdate.month < 10:
            x = "0" + str(sdate.month)
            y = str(sdate.year)
            list_date.append({x:y})
        else:
            x = str(sdate.month)
            y = str(sdate.year)
            list_date.append({x:y})
        sdate = sdate + relativedelta(months=1)
    yield list_date


def generate_list_file_path(start_date: str) -> list:
    base_path = r"G:\Google Play Console Sale Reports\zipfile\sales"
    list_file_path = []
    for i in next(generate_list_date(start_date)):
        for m, y in i.items():
            file_path = base_path + "\salesreport_" + y + m + ".zip"
            list_file_path.append(file_path)
    yield list_file_path 


def extract_file(start_date: str):
    for i in next(generate_list_file_path(start_date)):
        with ZipFile(i, "r") as file:
            file.extractall(path_csvfile_inlocal)
    
def load_csv_file_to_bucket(path_from, path_to):
    command = create_command( path_from, path_to)
    subprocess.call(command, shell = True)
#     os.system(r'cmd /k gsutil -m cp -r "C:\Users\admin\Desktop\phucdee\extracted_sales" gs://voliovn_app/sales')

if __name__ == "__main__":
    copy_file_from_GPC(path_google_play_console_sale_reports, path_savefile_in_bucket)
    download_file_from_bucket(path_zipfile_in_bucket, path_zipfile_in_local)
    check_file_download_succeed(file_path_to_check)
    extract_file(start_date)
    load_csv_file_to_bucket(path_csvfile_inlocal, path_csvfile_in_bucket)
