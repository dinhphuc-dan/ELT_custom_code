from google.cloud import storage, bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
import json
from pathlib import Path
from io import BytesIO
import re
from zipfile import ZipFile
from prefect import flow, task, get_run_logger
from prefect.blocks.notifications import SlackWebhook
import datetime


load_dotenv(
    override=True
)  # take environment variables from .env file for local testing
cwd: str = Path(__file__).parent.absolute()  # current working directory
base_local_path = rf"{cwd}"


class Google_IAP_to_Bigquery:
    def __init__(
        self,
        store_name,
        slack_channel,
        google_play_bucket,
        service_account,
        project_id,
        dataset_id,
        logger,
    ):
        self.store_name = store_name.lower()
        self.slack_channel = slack_channel
        self.google_play_bucket = google_play_bucket
        self.service_account = service_account
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.logger = logger

    def slack_noti(self, text_input):
        slack_webhook_block = SlackWebhook.load(f"{self.slack_channel}")
        slack_webhook_block.notify(f"{self.store_name} {text_input}")

    def check_IAP_from_console_then_load_to_bigquery(self):
        """Step 1: In case there is no file in console, then stop the process, else go next"""
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(self.service_account)
        )
        bucket = storage.Client(credentials=credentials).bucket(self.google_play_bucket)
        list_files_in_bucket: list = [
            x.name for x in bucket.list_blobs(prefix="sales/")
        ]  # get list file in storage
        self.logger.info(
            f"{self.store_name} Number file in Bucket: {len(list_files_in_bucket)}"
        )
        if len(list_files_in_bucket) == 0:
            raise FileNotFoundError
        # get list of date from Console file by list comprehension and then turn it into a set to remove duplication.
        # After that using sorting fuction to make a list of date in decending order
        list_date_in_google_console_files: list = sorted(
            {re.search(r"\d{6}", file).group() for file in list_files_in_bucket},
            reverse=True,
        )

        '''Step 2: Check table in Bigquery, if there is exist table then we get the missing file in Bigquery comparing to Console"""'''
        bqclient = bigquery.Client(credentials=credentials)  # make api call to bigquery
        dataset: str = f"{self.project_id}.{self.dataset_id}"  # set dataset
        list_tables_in_dataset: list = bqclient.list_tables(dataset=dataset)
        list_tables_in_dataset: list = [
            x.table_id
            for x in list_tables_in_dataset
            if re.search(r"salesreport_", x.table_id)
        ]

        self.logger.info(
            f"{self.store_name} Number table in Bigquery: {len(list_tables_in_dataset)}"
        )
        if len(list_tables_in_dataset) > 0:  # comparing between Console and Bigquery
            list_date_in_bigquery_table = sorted(
                {
                    re.search(r"\d{6}", table).group()
                    for table in list_tables_in_dataset
                },
                reverse=True,
            )
            list_date_not_in_bigquery: list = [
                x
                for x in list_date_in_google_console_files
                if x not in list_date_in_bigquery_table
            ]

        """ Step 3: If there is no table in Bigquery, then we will download all files from Console, unzip them then write to Bigquery
        else we write missing files and latest-month file to Bigquery """

        if len(list_tables_in_dataset) == 0:
            for file in list_files_in_bucket:
                response_in_bytes = BytesIO(
                    bucket.blob(file).download_as_bytes()
                )  # download file from Console as bytes then put it into BytesIO to make an in-memory file
                zip_file = ZipFile(response_in_bytes, "r")  # unzip file
                for name in zip_file.namelist():  # get each file in zip file
                    csv_file = BytesIO(
                        zip_file.read(name)
                    )  # read file from zip file as bytes then put it into BytesIO to make an in-memory file
                    bq_table_id = (
                        dataset + "." + re.match(r"[a-z0-9_]+", name).group()
                    )  # set bigquery table id
                    self.logger.info(
                        f"{self.store_name} List files append to Bigquery: {name}"
                    )
                    job_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.CSV,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        autodetect=True,
                    )
                    # Make an API request
                    load_job = bqclient.load_table_from_file(
                        file_obj=csv_file,
                        destination=bq_table_id,
                        job_config=job_config,
                    )
                    load_job.result()  # Waits for the job to complete
        else:
            list_date_not_in_bigquery.append(
                list_date_in_google_console_files[0]
            )  # append lastest-month file
            list_date_not_in_bigquery = set(
                list_date_not_in_bigquery
            )  # turn to set to make list unique
            self.logger.info(
                f"{self.store_name} List files append to Bigquery: {list_date_not_in_bigquery}"
            )
            for date in list_date_not_in_bigquery:
                file_name = f"sales/salesreport_{date}.zip"
                response_in_bytes = BytesIO(
                    bucket.blob(file_name).download_as_bytes()
                )  # download file from Console as bytes then put it into BytesIO to make an in-memory file
                zip_file = ZipFile(response_in_bytes, "r")  # unzip file
                for name in zip_file.namelist():  # get each file in zip file
                    csv_file = BytesIO(
                        zip_file.read(name)
                    )  # read file from zip file as bytes then put it into BytesIO to make an in-memory file
                    bq_table_id = (
                        dataset + "." + re.match(r"[a-z0-9_]+", name).group()
                    )  # set bigquery table id
                    self.logger.info(f"Appending to Bigquery: {bq_table_id}")
                    job_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.CSV,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        autodetect=True,
                    )
                    # Make an API request
                    load_job = bqclient.load_table_from_file(
                        file_obj=csv_file,
                        destination=bq_table_id,
                        job_config=job_config,
                    )
                    load_job.result()  # Waits for the job to complete


"""main flow"""


@flow(timeout_seconds=3600)
def IAP_main_flow():
    logger = (
        get_run_logger()
    )  # setup a logger instance from prefect.io, must stay inside a flow or a task of prefect
    list_store = json.loads(os.getenv("LIST_STORE"))
    list_instances = []
    for store in list_store:
        store_name = store
        SLACK_CHANNEL_env_name = store + "_" + "SLACK_CHANNEL"
        SLACK_CHANNEL = os.getenv(SLACK_CHANNEL_env_name)
        GOOGLE_PLAY_BUCKET_env_name = store + "_" + "GOOGLE_PLAY_BUCKET"
        GOOGLE_PLAY_BUCKET = os.getenv(GOOGLE_PLAY_BUCKET_env_name)
        SERVICE_ACCOUNT_env_name = store + "_" + "SERVICE_ACCOUNT"
        SERVICE_ACCOUNT = os.getenv(SERVICE_ACCOUNT_env_name)
        PROJECT_ID_env_name = store + "_" + "PROJECT_ID"
        PROJECT_ID = os.getenv(PROJECT_ID_env_name)
        DATASET_ID_env_name = store + "_" + "DATASET_ID"
        DATASET_ID = os.getenv(DATASET_ID_env_name)
        list_instances.append(
            Google_IAP_to_Bigquery(
                store_name=store_name,
                slack_channel=SLACK_CHANNEL,
                google_play_bucket=GOOGLE_PLAY_BUCKET,
                service_account=SERVICE_ACCOUNT,
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                logger=logger,
            )
        )

    for instance in list_instances:
        instance.slack_noti(text_input="IAP Start")
        instance.check_IAP_from_console_then_load_to_bigquery()
        instance.slack_noti(text_input="IAP Completed")


if __name__ == "__main__":
    IAP_main_flow()
