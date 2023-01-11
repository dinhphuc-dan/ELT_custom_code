from google.cloud import bigquery
from google.oauth2 import service_account





def transform_adjust_raw():
    credentials = service_account.Credentials.from_service_account_file(r'G:\Service Account Key\adjust\all-games-data-373009-19e7c3a80978.json')

    client = bigquery.Client(credentials=credentials)

    table_id = "all-games-data-373009.adjust_testing.test_table"

    job_config = bigquery.LoadJobConfig(
        autodetect = True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    uri = "gs://volio_group/zk1natw8prls_2023-01-09T060000_5d6abf6d806ec515709f3fc623630537_c3575d.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    # load_job.result()

    try:
        load_job.result()  # Waits for the job to complete.
    except Exception as e:
         print(e)
         



    destination_table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {destination_table.num_rows} rows.")

    # print(credentials)

    # bqclient = bigquery.Client(credentials=credentials)

    # query_job = bqclient.query(
    #     f"""
    #     SELECT *
    #     FROM `all-games-data-373009.adjust_testing.test_table`
    #     LIMIT 10"""
    # )

    # results = query_job.result()  # Waits for job to complete.

    # print(f'{next(results)}')


if __name__ == "__main__":
    transform_adjust_raw()