from pathlib import Path
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv
import os
import json
from prefect import task, flow, get_run_logger
import datetime
from prefect.blocks.notifications import SlackWebhook

cwd: str = Path(__file__).parent.absolute()
load_dotenv()

''' config logger '''
# logger = logging.getLogger(__name__)
# log_format = "%(asctime)s  -  %(levelname)s  -  %(name)s  -  %(filename)s  -  %(message)s"
# logger.setLevel('DEBUG')

''' Use FileHandler() to log to a file '''
# file_handler = logging.FileHandler("mylogs.log")
# formatter = logging.Formatter(log_format)
# file_handler.setFormatter(formatter)

''' Add console log '''
# console = logging.StreamHandler()
# console.setFormatter(logging.Formatter(log_format))

''' Don't forget to add the file handler into the logger '''
# # logger.addHandler(file_handler)
# logger.addHandler(console)

""" basic task for slack noti"""
@task(name = "Slack Noti")
def slack_noti(TextInput,AppName=os.getenv('APP_NAME')):
    slack_webhook_block = SlackWebhook.load("adjust")
    slack_webhook_block.notify(f"Flow {AppName} {TextInput}")

@flow(flow_run_name="{AppName}-on-{date:%Y-%m-%d-%H-%M-%S}",
      timeout_seconds=3600 
)
def transform_adjust_raw(AppName=os.getenv('APP_NAME'),
                         ServiceAccountEnv=os.getenv('SERVICE_ACCOUNT'),
                         BucketName=os.getenv('BUCKET_NAME'),
                         ProjectId=os.getenv('PROJECT_ID'),
                         Dataset=os.getenv('DATASET'),
                         date=datetime.datetime.now()
    ):
    logger = get_run_logger()

    list_date_from_file_name: list = []
    list_files_as_dict: dict = {}
    list_latest_csv_file: list = []
    adjust_export_schema: set = set()
    list_all_csv_file_with_latest_adjust_schema: list = []

    ''' storage credential'''
    service_account_info = json.loads(ServiceAccountEnv)
    # service_account_info = json.load(open(r'C:\Users\phucdinh\Desktop\ball-run-2048-c8459-b2f2fad569ed.json'))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bucket_name = BucketName.strip()
    bucket_client = storage.Client(credentials=credentials) # make api call to storage
    list_files_in_bucket = bucket_client.list_blobs(bucket_name) # get list file in storage

    
    # for file in list_files_in_bucket:
    #     list_files_as_dict.update({file.name:file.name.split('_')})
    #     date_from_file_name = file.name.split('_')[1]
    #     list_date_from_file_name.append(date_from_file_name)

    # select only newest csv file 
    # for k,v in list_files_as_dict.items():
    #     if v[1] == max(list_date_from_file_name):
    #         adjust_export_schema.add(v[2])
    #         list_latest_csv_file.append(k)

    # handle list of file in bucket
    for file in list_files_in_bucket:
        list_files_as_dict.update({file.name:file.name.split('_')})
        date_from_file_name:str = file.name.split('_')[1]
        date_from_file_name_as_datetime =  datetime.datetime.strptime(date_from_file_name, '%Y-%m-%dT%H%M%S')
        list_date_from_file_name.append(date_from_file_name_as_datetime)

    # select only newest csv file 
    for k,v in list_files_as_dict.items():
        if  datetime.datetime.strptime(v[1], '%Y-%m-%dT%H%M%S') >= (max(list_date_from_file_name) - datetime.timedelta(hours=8)):
            list_latest_csv_file.append(k)
            if datetime.datetime.strptime(v[1], '%Y-%m-%dT%H%M%S')==(max(list_date_from_file_name)):
                adjust_export_schema.add(v[2])
            
    # if has more than 1 schema from adjust, raise error and stop running code, this case happen in the first hour after change schema 
    if len(adjust_export_schema) > 1:
        raise FileExistsError
    else:
        pass
    
    # item here is only the latest schema
    for item in adjust_export_schema:
        for k,v in list_files_as_dict.items():
            if v[2] == item:
                schema_id_from_adjust = item
                list_all_csv_file_with_latest_adjust_schema.append(k)

    ''' bigquery credential'''
    bqclient = bigquery.Client(credentials=credentials)
    table_id = f"{ProjectId}.{Dataset}.{schema_id_from_adjust}" # set table id with schema hash from adjust

    schema_path = f"{cwd}/schema.json"

    schema = bqclient.schema_from_json(schema_path)

    try:
        logger.info('RUN TRY')
        destination_table = bqclient.get_table(table_id) # Make an API request.
        logger.info(f'destination table row before append - {destination_table.num_rows} ')
        logger.info(f' number of files to append - {len(list_latest_csv_file)}')
        for f in list_latest_csv_file:
            file_uri = f"gs://{bucket_name}/" + f"{f}"
            logger.info(f'append file - {f} ')
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            # Make an API request.
            load_job = bqclient.load_table_from_uri(
                file_uri, table_id, job_config=job_config
            )
            load_job.result()  # Waits for the job to complete.
        destination_table = bqclient.get_table(table_id) # Make an API request.
        logger.info(f'destination table row after append- {destination_table.num_rows} ')

    # when table is not created, then run the below code
    except NotFound:
        logger.info('RUN EXCEPT')
        logger.info(f' number of files to append - {len(list_all_csv_file_with_latest_adjust_schema)}')
        for f in list_all_csv_file_with_latest_adjust_schema:
            file_uri = f"gs://{bucket_name}/" + f"{f}"
            logger.info(f'append file - {f} ')
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            # Make an API request.
            load_job = bqclient.load_table_from_uri(
                file_uri, table_id, job_config=job_config
            )
            load_job.result()  # Waits for the job to complete.
        destination_table = bqclient.get_table(table_id)
        logger.info(f'destination table row after append- {destination_table.num_rows} ')

@flow(flow_run_name="{AppName}-on-{date:%Y-%m-%d-%H-%M-%S}",
      timeout_seconds=3600,
      retries=0,
      retry_delay_seconds=10   
)
def main_flow(AppName=os.getenv('APP_NAME'), date=datetime.datetime.now()):
    slack_noti("Run")
    transform_adjust_raw()
    slack_noti("Completed")

if __name__ == "__main__":
    main_flow()