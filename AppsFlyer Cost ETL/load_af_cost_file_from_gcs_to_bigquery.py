from google.cloud import storage, bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
import json
import datetime
import re
from prefect import task, flow, get_run_logger
from prefect.blocks.notifications import SlackWebhook



'''
    AppsFlyer Cost ETL send 4 batchs to bucket every days. The 4th one contains all data for that day, so we just need to get data from all file in 4th batch
    Addtionally,we only get data from channel dimensions because we want to get to lowest level in campaign cost which is channel or ads or adset
    The table name in Bigquery will be like: appsflyer_cost_channel_2023-08-30
    You need to put info into an .env file like file .env.md 
'''

load_dotenv() # take environment variables from .env file for local testing


""" basic task for slack noti"""
@task(name = "Slack Noti")
def slack_noti(text_input):
    slack_webhook_block = SlackWebhook.load("appsflyer")
    slack_webhook_block.notify(f"Flow {text_input}")

@flow(flow_run_name="AppsFlyer-cost-on-{date:%Y-%m-%d-%H-%M-%S}",
      timeout_seconds=3600,
      retries=1,
      retry_delay_seconds=10 
)
def load_af_cost_file_from_gcs_to_bigquery(ServiceAccountEnv=os.getenv('SERVICE_ACCOUNT'),
                         BucketName=os.getenv('BUCKET_NAME'),
                         ProjectId=os.getenv('PROJECT_ID'),
                         DatasetId=os.getenv('DATASET_ID'),
                         date=datetime.datetime.now()
    ):
    
    ''' Step 1: Set up Google credentials, bucket and variables '''
    # If you dont want to load info from .env file, you can load service account directly from your local file as 
    # json.load(open(r'your-file-path.json'))
    
    logger = get_run_logger() # setup a logger instance from prefect.io, must stay inside a flow or a task of prefect
    credentials  = service_account.Credentials.from_service_account_info(json.loads(ServiceAccountEnv))
    bucket_name: str = BucketName.strip()
    folder_name: str = "cost_etl/v1"
    

    ''' Step 2: Check if there are existing AF 4th batch files in your bucket'''

    ''' storage API'''
    bucket_client = storage.Client(credentials=credentials) # make api call to storage
    list_files_in_bucket: list = bucket_client.list_blobs(bucket_or_name=bucket_name, prefix=folder_name) # get list file in storage
    list_file_4th_batch: list = [x.name for x in list_files_in_bucket if re.search(r"b=4", x.name)] 
    logger.info('Check 4th batch in bucket')

    # if there is no 4th batch file, raise error and stop the flow
    if len(list_file_4th_batch) > 0:
        logger.info(f'Number of 4th batch file: {len(list_file_4th_batch)}')
    else:
        raise FileNotFoundError
    
    # get list of date from 4th batch file by list comprehension and then turn it into a set to remove duplication. 
    # After that using sorting fuction to make a list of date in decending order
    list_of_date_of_4th_batch_file: list = sorted({re.search(r'(\d{4}-\d{2}-\d{2})', file).group() for file in list_file_4th_batch}, reverse=True)

    ''' Step 3: Check if there are existing tables of AF in bigquery'''

    '''bigquery API'''
    bqclient = bigquery.Client(credentials=credentials) # make api call to bigquery
    dataset: str = f"{ProjectId}.{DatasetId}" # set dataset 
    list_tables_in_dataset: list = bqclient.list_tables(dataset=dataset)

    # check tables
    list_tables_in_dataset: list = [x.table_id for x in list_tables_in_dataset if re.search(r"appsflyer_cost_channel", x.table_id)]
    logger.info('Check tables in bigquery')

    ''' Step 4: If there is no table in bigquery or number of date in 4th batch file smaller than 5 days, then running full refresh - overwrite, if not overwrite the latest 5 days tables '''
    if len(list_tables_in_dataset) > 0 and len(list_of_date_of_4th_batch_file) >= 5:
        # running incremental overwrite the last 5 days tables
        logger.info('Run incremental overwrite the last 5 days tables')
        for number in range(0,5):
            table_id = dataset + ".appsflyer_cost_channel_" + list_of_date_of_4th_batch_file[number]
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            file_uri = f"gs://{bucket_name}/{folder_name}/dt={list_of_date_of_4th_batch_file[number]}/b=4/channel/*.parquet"

            # Make an API request
            load_job = bqclient.load_table_from_uri(file_uri, table_id, job_config=job_config)
            load_job.result()  # Waits for the job to complete
    else:
        # running full refresh overwrite all tables
        logger.info('Run full refresh overwrite all tables')
        for date in list_of_date_of_4th_batch_file:
            table_id = dataset + ".appsflyer_cost_channel_" + date
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            file_uri = f"gs://{bucket_name}/{folder_name}/dt={date}/b=4/channel/*.parquet"

            # Make an API request
            load_job = bqclient.load_table_from_uri(file_uri, table_id, job_config=job_config)
            load_job.result()  # Waits for the job to complete

@flow(flow_run_name="AppsFlyer-cost-on-{date:%Y-%m-%d-%H-%M-%S}",
      timeout_seconds=3600  
)
def AppsFlyer_cost_main_flow(date=datetime.datetime.now()):
    slack_noti("Run load_af_cost_file_from_gcs_to_bigquery")
    load_af_cost_file_from_gcs_to_bigquery()
    slack_noti("Completed load_af_cost_file_from_gcs_to_bigquery")

if __name__ == "__main__":
    AppsFlyer_cost_main_flow()