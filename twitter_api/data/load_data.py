from google.cloud import bigquery
import time


def load_data_from_file(dataset_name, table_name, source_file_name):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)

    # Reload the table to get the schema.
    table.reload()

    with open(source_file_name, 'rb') as source_file:
        # This example uses CSV, but you can use other formats.
        # See https://cloud.google.com/bigquery/loading-data
        job = table.insert_data().upload_from_file(
            source_file, source_format='text/csv')

    wait_for_job(job)

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))


def load_data_from_records(dataset_name, table_name, records):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)

    # Reload the table to get the schema.
    table.reload()

    errors = table.insert_data(records)

    if not errors:
        print('Loaded {} row into {}:{}'.format(len(records), dataset_name, table_name))
    else:
        print('Errors:')
        print(str(errors))


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


# load_data_from_file('gap_social_organic', 'org_ig', '../outputs/organic/org_ig_052317.csv')

