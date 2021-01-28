import os
import logging
import uuid
from datetime import datetime
from utils import copy_s3_file, utl_create_source2parquet_log_entry


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("************* Start Job *************")

guid = str(uuid.uuid4().hex)
process_name = 'cheq_report_trigger'
sub_process_name = ''

# Lambda ENVIRONMENT VARIABLES
destination_bucket = os.environ['DESTINATION_BUCKET']


def lambda_handler(event, context):
    """ This function fetches object content to MySQL table source_file_aws_lambda_trigger and Trigger Airflow Dag"""

    try:

        insert_time = datetime.now()
        start_process_time = insert_time
        start_step_time = insert_time
        utl_create_source2parquet_log_entry(guid, process_name, '', 'general_step', 'start', insert_time,
                                            start_step_time, start_process_time, '')

        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            obj_size = record['s3']['object']['size']

            if key.endswith('.gz'):
                dt = key.split('/')[0]
                # hour = key.split('/')[1]
                source_file = key.split('/')[2]
                dt_without_hyphen = dt.replace("-", "")

                s3_source_bucket = bucket
                s3_source_key = key
                s3_destination_bucket = destination_bucket  # 'search-datalog-nonprod-us-east-1'
                s3_destination_key = 'datalogs/cheq_reports/{dt_in}/{source_file_in}'.format(dt_in=dt_without_hyphen,
                                                                                             source_file_in=source_file)
                sub_process_name = source_file
                insert_time = datetime.now()
                start_step_time = insert_time
                step_name = 'copy file from source bucket to target - source2parquet process'
                logger.info('************* ' + step_name + ' *************')
                utl_create_source2parquet_log_entry(guid, process_name, sub_process_name, step_name, 'start',
                                                    insert_time, start_step_time, start_process_time, '')

                copy_s3_file(s3_source_bucket, s3_source_key, s3_destination_bucket, s3_destination_key)

                utl_create_source2parquet_log_entry(guid, process_name, sub_process_name, step_name, 'success',
                                                    datetime.now(), start_step_time, start_process_time, '')

        utl_create_source2parquet_log_entry(guid, process_name, sub_process_name, 'general_step',
                                            'success', datetime.now(), start_process_time, start_process_time, '')

        logger.info("************* End Job *************")

    except Exception as e:
        logger.error(e)
        utl_create_source2parquet_log_entry(guid, process_name, sub_process_name, 'general_step', 'failure',
                                            datetime.now(), start_process_time, start_process_time, str(e))
        raise



