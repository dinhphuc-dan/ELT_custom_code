import os
from pathlib import Path
import subprocess
from zipfile import ZipFile
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook


""" define variable"""
# path
cwd: str = Path(__file__).parent.absolute()
base_local_path: str = fr"{cwd}\zipfile\sales"
path_google_play_console_sale_reports: str = r"gs://pubsite_prod_rev_13845136734357471158/sales"
path_savefile_in_bucket: str = r"gs://voliovn_app/wechoice"
path_zipfile_in_bucket: str = r"gs://voliovn_app/wechoice/sales"
path_csvfile_in_bucket: str = r"gs://voliovn_app/wechoice/csv_sales"
path_zipfile_in_local: str = fr"{cwd}\zipfile"
path_csvfile_inlocal: str = fr"{cwd}\unzipfile\csv_sales"
file_path_to_check: str = fr"{cwd}\zipfile\sales\salesreport_202301.zip"

#date
start_date:str = '2022-12-01'
last_month_date: str = datetime.strftime((datetime.today() - relativedelta(months=1)),'%Y-%m-%d')

#bigquery config
email_bigquery_owner: str = "airbytebigquery@pdf-reader-2-ab0b8.iam.gserviceaccount.com"
project_id = "pdf-reader-2-ab0b8"
dataset_id = "local_raw_wechoice_in_app_purchase_all_apps"


"""function task which does not need to tracking"""
@task
def change_gcloud_account_command(account: str)-> str:
    return fr'gcloud config set account {account}'

@task
def create_gsutil_command(path_from: str, path_to: str)-> str:
    return fr'gsutil -m cp -r "{path_from}" "{path_to}"'

@task
def create_bigquery_delete_table_cmd(project_id: str, dataset_id: str, table_name:str)-> str:
    return fr'bq rm -f -t --project_id={project_id} {dataset_id}.{table_name}'

@task
def create_bigquery_load_table_cmd(project_id: str, dataset_id: str, table_name:str, path:str)-> str:
    return fr'bq load --autodetect --source_format=CSV --project_id={project_id} {dataset_id}.{table_name} "{path}" '

@task
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

@task
def generate_list_file_path(base_path:str ,start_date: str) -> list:
    list_file_path = []
    for i in next(generate_list_date.fn(start_date=start_date)):
        for m, y in i.items():
            file_path = base_path + "\salesreport_" + y + m + ".zip"
            list_file_path.append(file_path)
    yield list_file_path 

@task
def generate_table_name(start_date):
    list_table_name = []
    for i in next(generate_list_date.fn(start_date=start_date)):
        for m, y in i.items():
            file_path = "salesreport_" + y + m
            list_table_name.append(file_path)
    yield list_table_name

"""main task"""
# basic task for slack noti
@task(name = "Slack Noti")
def slack_noti(text_input):
    slack_webhook_block = SlackWebhook.load("wechoice")
    slack_webhook_block.notify(f"{text_input}")

@task
def check_file_download_succeed(file_path)->bool:
    modified_date = datetime.fromtimestamp(os.stat(file_path).st_mtime)
    modified_date_str:str = datetime.strftime(modified_date,"%Y-%m-%d")
    today_str:str = datetime.strftime(date.today(),"%Y-%m-%d")
    if today_str == modified_date_str:
        pass
    else:
        raise Exception("Error in Download")

"""sub flow"""
@task
def change_gcloud_account(account):
    command = change_gcloud_account_command.fn(account)
    subprocess.call(command, shell = True)

@task
def copy_file_from_GPC(path_from, path_to):
    command = create_gsutil_command.fn(path_from, path_to)
    subprocess.call(command, shell = True)

@task
def download_file_from_bucket(path_from, path_to):
    command = create_gsutil_command.fn(path_from, path_to)
    subprocess.call(command,shell = True)
    # os.system('cmd /c gsutil -m cp -r  gs://voliovn_app/sales "G:\Google Play Console Sale Reports\zipfile"')

@task
def extract_file(base_path:str ,start_date: str):
    for i in next(generate_list_file_path.fn(base_path=base_path,start_date=start_date)):
        with ZipFile(i, "r") as file:
            file.extractall(path_csvfile_inlocal)

@task
def load_csv_file_to_bucket(path_from, path_to):
    command = create_gsutil_command.fn( path_from=path_from, path_to=path_to)
    subprocess.call(command, shell = True)
#     os.system(r'cmd /k gsutil -m cp -r "C:\Users\admin\Desktop\wechoice\extracted_sales" gs://voliovn_app/sales')

@task
def delete_all_table_bigquery(project_id: str, dataset_id: str, start_date:str):
    for table_name in next(generate_table_name.fn(start_date=start_date)):
        command = create_bigquery_delete_table_cmd.fn(project_id=project_id, dataset_id=dataset_id, table_name=table_name)
        subprocess.call(command, shell = True)

@task
def load_all_table_bigquery(project_id: str, dataset_id: str, start_date:str, path:str):
    for table_name in next(generate_table_name.fn(start_date=start_date)):
        final_path = fr"{path}/{table_name}.csv"
        command = create_bigquery_load_table_cmd.fn(project_id=project_id, dataset_id=dataset_id, table_name=table_name, path=final_path)
        subprocess.call(command, shell = True)

"""main flow"""
@flow(timeout_seconds=3600)
def wechoice_7h20_Google_Play_Console_sale_reports():
    slack_noti("Flow wechoice-Google Play Console Report start running")
    change_gcloud_account(account=email_bigquery_owner)
    copy_file_from_GPC(path_from=path_google_play_console_sale_reports, path_to=path_savefile_in_bucket)
    download_file_from_bucket(path_from=path_zipfile_in_bucket, path_to=path_zipfile_in_local)
    check_file_download_succeed(file_path_to_check)
    extract_file(base_path= base_local_path,start_date=start_date)
    load_csv_file_to_bucket(path_from=path_csvfile_inlocal, path_to=path_savefile_in_bucket)
    delete_all_table_bigquery(project_id=project_id, dataset_id=dataset_id, start_date=last_month_date)
    load_all_table_bigquery(project_id=project_id, dataset_id=dataset_id, start_date=last_month_date, path=path_csvfile_in_bucket )
    slack_noti("Flow wechoice-Google Play Console Report run successfully")

if __name__ == "__main__":
    wechoice_7h20_Google_Play_Console_sale_reports()