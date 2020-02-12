import boto3
import logging

from datetime import date

client = boto3.client('glue')


def handler(event, context):
    """ Create/Run a crawler/job to perform ETL operation limited to
        conversion of json-parquet format

    """
    # Initialization
    timestamp = date.today()
    bucket_event_record = event['Records'][0]['s3']
    crawler_name = 'albedo-joined-feeds-data-dump'
    job_name = 'albedo-feeds-json-to-parquet'
    aws_role = 'arn:aws:iam::143032791481:role/service-role/AWSGlueServiceRole-albedo_with_feeds'
    script_path = 's3://bbhate-gluetl-script-json-to-parquet/script/albedo-feeds-json-to-parquet'
    bucket_path = (
        's3://{}/{}'.format(bucket_event_record['bucket']['name'], bucket_event_record['object']['key'][:11]))
    db_name = 'lo_werphat__analyst_db'
    table_prefix = ('lo_werphat__albedo_to_athena_{}_{}_'.format(
        timestamp.strftime('%Y'), timestamp.strftime('%m')))

    # Check if crawler is already created, if yes then start crawling
    if crawler_name in client.list_crawlers()['CrawlerNames']:
        client.update_crawler(
            Name=crawler_name,
            Role=aws_role,
            DatabaseName=db_name,
            TablePrefix=table_prefix,
            Targets={
                'S3Targets': [
                    {
                        'Path': bucket_path,
                    },
                ]
            }
        )

        client.start_crawler(Name=crawler_name)
        logging.info('## CRAWLER RUNNING')

    else:
        # Create Crawler
        client.create_crawler(
            Name=crawler_name,
            Role=aws_role,
            DatabaseName=db_name,
            TablePrefix=table_prefix,
            Targets={
                'S3Targets': [
                    {
                        'Path': bucket_path,
                    },
                ]
            }
        )
        logging.info('## CRAWLER CREATED')

        # Start Crawler
        client.start_crawler(Name=crawler_name)
        logging.info('## CRAWLER RUNNING')

    # Check if a ETL job is created, if yes then start job
    if job_name in client.list_jobs()['JobNames']:
        client.start_job_run(JobName=job_name)
        logging.info('## JOB RUNNING')

    else:
        # Create a ETL job
        client.create_job(
            Name=job_name,
            Role=aws_role,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_path
            }
        )
        logging.info('## JOB CREATED')

        # Start the job
        client.start_job_run(
            JobName=job_name)
        logging.info('## JOB RUNNING')
    response = {
        'statusCode': 200,
    }
    return response
