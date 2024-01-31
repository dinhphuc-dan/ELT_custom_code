from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound, BadRequest
from dotenv import load_dotenv
import os
import json
from pathlib import Path
from io import BytesIO, StringIO
import re
from zipfile import ZipFile
from datetime import datetime, date
import pendulum
import csv


load_dotenv() # take environment variables from .env file for local testing
cwd: str = Path(__file__).parent.absolute() # current working directory
base_local_path = fr'{cwd}'

class GoogleConsoleAggregatedReportsToBigquery():
        def __init__(self, store_name, google_play_bucket, service_account, project_id, dataset_id, start_date, end_date, list_package):
                
                self.store_name: str = store_name.lower()
                self.google_play_bucket: str = google_play_bucket
                self.service_account: str = service_account
                self.project_id: str = project_id
                self.dataset_id: str = dataset_id
                self.start_date: date = pendulum.parse(start_date)
                self.end_date: date = pendulum.parse(end_date)
                self.list_package: list = json.loads(list_package)

        def load_file_from_console_to_bigquery(self):
                credentials = service_account.Credentials.from_service_account_info(json.loads(self.service_account))
                bucket = storage.Client(credentials=credentials).bucket(self.google_play_bucket)
                bqclient = bigquery.Client(credentials=credentials) # make bigquery connection
                dataset: str = f"{self.project_id}.{self.dataset_id}" # set dataset 

                for package in self.list_package:
                        for date in [self.start_date.add(months = n).format('YYYYMM') for n in range((self.end_date.diff(self.start_date).in_months() + 1))]:
                                file_name = f'stats/installs/installs_{package}_{date}_overview.csv'
                                try: 
                                        response_in_string_file: csv = StringIO(bucket.blob(file_name).download_as_text())
                                        bq_table_id = dataset + '.' + f'overview_installs_{package.replace(".","_")}_{date}'
                                        print(bq_table_id)
                                        job_config = bigquery.LoadJobConfig(
                                                source_format=bigquery.SourceFormat.CSV,
                                                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                                autodetect=True
                                        )
                                        # Make an API request
                                        load_job = bqclient.load_table_from_file(file_obj= response_in_string_file, destination=bq_table_id, job_config=job_config)
                                        load_job.result()  # Waits for the job to complete
                                        print(f'Loaded {file_name} into {bq_table_id}.')
                                except NotFound:
                                        pass
                                except BadRequest:
                                        response_in_string_file: csv = StringIO(bucket.blob(file_name).download_as_text())
                                        bq_table_id = dataset + '.' + f'overview_installs_{package.replace(".","_")}_{date}'
                                        print(bq_table_id)
                                        job_config = bigquery.LoadJobConfig(
                                                source_format=bigquery.SourceFormat.CSV,
                                                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                                autodetect=True
                                        )
                                        # Make an API request
                                        load_job = bqclient.load_table_from_file(file_obj= response_in_string_file, destination=bq_table_id, job_config=job_config)
                                        load_job.result()  # Waits for the job to complete
                                        print(f'Loaded {file_name} into {bq_table_id}.')

def main_flow():
    list_store = json.loads(os.getenv('LIST_STORE'))
    list_instances = []
    for store in list_store:
        store_name = store
        GOOGLE_PLAY_BUCKET_env_name = store + '_' +'GOOGLE_PLAY_BUCKET'
        GOOGLE_PLAY_BUCKET = os.getenv(GOOGLE_PLAY_BUCKET_env_name)
        SERVICE_ACCOUNT_env_name = store + '_' +'SERVICE_ACCOUNT'
        SERVICE_ACCOUNT = os.getenv(SERVICE_ACCOUNT_env_name)
        PROJECT_ID_env_name = store + '_' +'PROJECT_ID'
        PROJECT_ID = os.getenv(PROJECT_ID_env_name)
        DATASET_ID_env_name = store + '_' +'DATASET_ID'
        DATASET_ID = os.getenv(DATASET_ID_env_name)
        START_DATE = os.getenv('START_DATE')
        END_DATE = os.getenv('END_DATE')
        LIST_APP_PACKAGE = os.getenv('LIST_APP_PACKAGE')
        list_instances.append(GoogleConsoleAggregatedReportsToBigquery(
                                store_name=store_name,
                                google_play_bucket=GOOGLE_PLAY_BUCKET,
                                service_account=SERVICE_ACCOUNT,
                                project_id=PROJECT_ID,
                                dataset_id=DATASET_ID,
                                start_date=START_DATE,
                                end_date=END_DATE,
                                list_package=LIST_APP_PACKAGE
                            ))

    for instance in list_instances:
        instance.load_file_from_console_to_bigquery()
   
if __name__ == "__main__":
    main_flow()
