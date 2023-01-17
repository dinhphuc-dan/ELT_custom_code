from pathlib import Path
import logging
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

cwd: str = Path(__file__).parent.absolute()

''' config logger '''
logger = logging.getLogger(__name__)
log_format = "%(asctime)s  -  %(levelname)s  -  %(name)s  -  %(filename)s  -  %(message)s"

''' To override the default severity of logging '''
logger.setLevel('DEBUG')

''' Use FileHandler() to log to a file '''
# file_handler = logging.FileHandler("mylogs.log")
# formatter = logging.Formatter(log_format)
# file_handler.setFormatter(formatter)

''' Add console log '''
console = logging.StreamHandler()
console.setFormatter(logging.Formatter(log_format))

''' Don't forget to add the file handler into the logger '''
# logger.addHandler(file_handler)
logger.addHandler(console)


def transform_adjust_raw():
    list_date_from_file_name: list = []
    list_files_as_dict: dict = {}
    list_latest_csv_file: list = []
    adjust_export_schema: set = set()
    list_csv_file_with_latest_adjust_schema: list = []
    credentials = service_account.Credentials.from_service_account_file(r'G:\Service Account Key\adjust\Jacat\all-games.json')

    bucket_name = 'volio_group'

    bucket_client = storage.Client(credentials=credentials)

    list_files_in_bucket = bucket_client.list_blobs(bucket_name)
    for file in list_files_in_bucket:
        list_files_as_dict.update({file.name:file.name.split('_')})
        date_from_file_name = file.name.split('_')[1]
        list_date_from_file_name.append(date_from_file_name)

    for k,v in list_files_as_dict.items():
        if v[1] == max(list_date_from_file_name):
            adjust_export_schema.add(v[2])
            list_latest_csv_file.append(k)

    if len(adjust_export_schema) > 1:
        raise FileExistsError
    else:
        pass
    
    for item in adjust_export_schema:
        for k,v in list_files_as_dict.items():
            if v[2] == item:
                list_csv_file_with_latest_adjust_schema.append(k)

    logger.info(f' number of files to append - {len(list_latest_csv_file)} ')

    bqclient = bigquery.Client(credentials=credentials)

    table_id = "all-games-data-373009.adjust_testing.test9_table"

    schema_path = f"{cwd}/schema.json"

    schema = bqclient.schema_from_json(schema_path)

    try:
        logger.info('RUN TRY')
        destination_table = bqclient.get_table(table_id) # Make an API request.
        logger.info(f'destination table row before append - {destination_table.num_rows} ')
        for f in list_latest_csv_file:
            file_uri = "gs://volio_group/" + f"{f}"
            
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

    except NotFound:
        logger.info('RUN EXCEPT')
        for f in list_csv_file_with_latest_adjust_schema:
            file_uri = "gs://volio_group/" + f"{f}"
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

if __name__ == "__main__":
    transform_adjust_raw()