from pathlib import Path
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv
import os
import json
from prefect import task, flow, get_run_logger
import datetime
import re
# from prefect.blocks.notifications import SlackWebhook
import pendulum


'''
    AppsFlyer Cost ETL send 4 batchs to bucket every days. The 4th one contains all data for that day, so we just need to get data from all file in 4th batch
    Addtionally,we only get data from channel dimensions because we want to get to lowest level in campaign cost which is channel or ads or adset
    The table name in Bigquery will be like: appsflyer_cost_channel_2023-08-30
    You need to put info into an .env file like file .env.md 
'''

load_dotenv() # take environment variables from .env file for local testing
# logger = get_run_logger() # setup a logger instance from prefect.io

# """ basic task for slack noti"""
# @task(name = "Slack Noti")
# def slack_noti(text_input):
#     slack_webhook_block = SlackWebhook.load("appsflyer")
#     slack_webhook_block.notify(f"Flow {text_input}")

# @flow(flow_run_name="AppsFlyer-cost-on-{date:%Y-%m-%d-%H-%M-%S}",
#       timeout_seconds=3600,
#       retries=1,
#       retry_delay_seconds=10 
# )
def load_af_cost_file_from_gcs_to_bigquery(ServiceAccountEnv=os.getenv('SERVICE_ACCOUNT'),
                         BucketName=os.getenv('BUCKET_NAME'),
                         ProjectId=os.getenv('PROJECT_ID'),
                         Dataset=os.getenv('DATASET'),
                         date=datetime.datetime.now()
    ):
    
    ''' Step 1: Set up Google credentials, bucket and variables '''
    # If you dont want to load info from .env file, you can load service account directly from your local file as 
    # json.load(open(r'your-file-path.json'))
    
    credentials  = service_account.Credentials.from_service_account_info(json.loads(ServiceAccountEnv))
    bucket_name: str = BucketName.strip()
    folder_name: str = "cost_etl/v1"
    

    ''' Step 2: Check if there are existing AF 4th batch files in your bucket'''

    ''' storage API'''
    bucket_client = storage.Client(credentials=credentials) # make api call to storage
    print('call API good')
    list_files_in_bucket: list = bucket_client.list_blobs(bucket_or_name=bucket_name, prefix=folder_name) # get list file in storage
    list_file_4th_batch: list = [x.name for x in list_files_in_bucket if re.search(r"b=4", x.name)] 

    # if there is no 4th batch file, raise error and stop the flow
    if len(list_file_4th_batch) > 0:
        print(len(list_file_4th_batch))
    else:
        raise FileNotFoundError
    
    # get list of date from 4th batch file
    set_of_date_of_4th_batch_file: set = {re.search(r'(\d{4}-\d{2}-\d{2})', file).group() for file in list_file_4th_batch}
    print(set_of_date_of_4th_batch_file)

    ''' Step 3: Check if there are existing tables of AF in bigquery'''

    '''bigquery API'''
    bqclient = bigquery.Client(credentials=credentials) # make api call to bigquery
    dataset: str = f"{ProjectId}.{Dataset}" # set dataset 
    list_tables_in_dataset: list = bqclient.list_tables(dataset=dataset)

    # check tables
    list_tables_in_dataset: list = [x.table_id for x in list_tables_in_dataset if re.search(r"appsflyer_cost_channel", x.table_id)]
    print(list_tables_in_dataset)

    ''' Step 4: If there is no table in bigquery, running full refresh - overwrite '''
    if len(list_tables_in_dataset) == 0:
        for date in set_of_date_of_4th_batch_file:
            table_id = dataset + ".appsflyer_cost_channel_test_" + date
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            file_uri = f"gs://{bucket_name}/{folder_name}/dt={date}/b=4/channel/*.parquet"

            # Make an API request
            load_job = bqclient.load_table_from_uri(file_uri, table_id, job_config=job_config)
            load_job.result()  # Waits for the job to complete


    # ''' bigquery credential'''
    # bqclient = bigquery.Client(credentials=credentials)
    # table_id = f"{ProjectId}.{Dataset}.{schema_id_from_adjust}" # set table id with schema hash from adjust

    # schema_path = f"{cwd}/schema.json"

    # schema = bqclient.schema_from_json(schema_path)

    # try:
    #     logger.info('RUN TRY')
    #     destination_table = bqclient.get_table(table_id) # Make an API request.
    #     logger.info(f'destination table row before append - {destination_table.num_rows} ')
    #     logger.info(f' number of files to append - {len(list_latest_csv_file)}')
    #     for f in list_latest_csv_file:
    #         file_uri = f"gs://{bucket_name}/" + f"{f}"
            
    #         logger.info(f'append file - {f} ')
    #         job_config = bigquery.LoadJobConfig(
    #             schema=schema,
    #             skip_leading_rows=1,
    #             source_format=bigquery.SourceFormat.CSV,
    #             write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    #             time_partitioning=bigquery.TimePartitioning(
    #                 type_=bigquery.TimePartitioningType.DAY,
    #                 field="_created_at_"
    #                 ) 
    #         )
    #         # Make an API request.
    #         load_job = bqclient.load_table_from_uri(
    #             file_uri, table_id, job_config=job_config
    #         )
    #         load_job.result()  # Waits for the job to complete.
    #     destination_table = bqclient.get_table(table_id) # Make an API request.
    #     logger.info(f'destination table row after append- {destination_table.num_rows} ')

    # # when table is not created, then run the below code
    # except NotFound:
    #     logger.info('RUN EXCEPT')
    #     logger.info(f' number of files to append - {len(list_all_csv_file_with_latest_adjust_schema)}')
    #     for f in list_all_csv_file_with_latest_adjust_schema:
    #         file_uri = f"gs://{bucket_name}/" + f"{f}"
    #         logger.info(f'append file - {f} ')
    #         job_config = bigquery.LoadJobConfig(
    #             schema=schema,
    #             skip_leading_rows=1,
    #             source_format=bigquery.SourceFormat.CSV,
    #             write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    #             time_partitioning=bigquery.TimePartitioning(
    #                 type_=bigquery.TimePartitioningType.DAY,
    #                 field="_created_at_"
    #                 ) 
    #         )
    #         # Make an API request.
    #         load_job = bqclient.load_table_from_uri(
    #             file_uri, table_id, job_config=job_config
    #         )
    #         load_job.result()  # Waits for the job to complete.
    #     destination_table = bqclient.get_table(table_id)
    #     logger.info(f'destination table row after append- {destination_table.num_rows} ')

# @flow(flow_run_name="{AppName}-on-{date:%Y-%m-%d-%H-%M-%S}",
#       timeout_seconds=3600  
# )
def main_flow(date=datetime.datetime.now()):
    # slack_noti("Run")
    load_af_cost_file_from_gcs_to_bigquery()
    # slack_noti("Completed")

if __name__ == "__main__":
    main_flow()