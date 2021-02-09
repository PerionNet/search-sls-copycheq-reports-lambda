import os
import logging
import uuid
import urllib.parse
from datetime import datetime
import time
from utils import copy_s3_file,copy_s3_file2,  utl_create_source2parquet_log_entry


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

        # hot fix - wait 20 seconds until lambda starts
        time.sleep(20)
        txt_info = "Sleep for 20 seconds"
        logger.info(txt_info)
        utl_create_source2parquet_log_entry(guid, process_name, '', txt_info, 'info',
                                            datetime.now(), start_step_time, start_process_time, '')

        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            obj_size = record['s3']['object']['size']

            if key.endswith('.gz'):
                dt = key.split('/')[0]
                # hour = key.split('/')[1]
                source_file = key.split('/')[2]
                dt_without_hyphen = dt.replace("-", "")

                # change file prefix to _tsv.gz
                dest_file = source_file.replace(".gz", "_tsv.gz")

                s3_source_bucket = bucket
                s3_source_key = key
                s3_destination_bucket = destination_bucket  # 'search-datalog-nonprod-us-east-1'
                s3_destination_key = 'datalogs/cheq_report/{dt_in}/{dest_file_in}'.format(dt_in=dt_without_hyphen,
                                                                                          dest_file_in=dest_file)
                sub_process_name = source_file
                insert_time = datetime.now()
                start_step_time = insert_time
                step_name = 'copy file from source bucket to target - source2parquet process'
                logger.info('************* ' + step_name + ' *************')
                utl_create_source2parquet_log_entry(guid, process_name, sub_process_name, step_name, 'start',
                                                    insert_time, start_step_time, start_process_time, '')

                # s3_source_key_new = s3_source_key.replace("%3D", "=")
                # s3_destination_key_new = s3_destination_key.replace("%3D", "=")

                s3_source_key_new = urllib.parse.unquote(s3_source_key)
                s3_destination_key_new = urllib.parse.unquote(s3_destination_key)

                logger.info('s3_source_bucket: {}'.format(s3_source_bucket))
                logger.info('s3_source_key: {}'.format(s3_source_key_new))
                logger.info('s3_destination_bucket: {}'.format(s3_destination_bucket))
                logger.info('s3_destination_key: {}'.format(s3_destination_key_new))

                copy_s3_file(s3_source_bucket, s3_source_key_new, s3_destination_bucket, s3_destination_key_new)

                utl_create_source2parquet_log_entry(guid, process_name, sub_process_name, step_name, 'success',
                                                    datetime.now(), start_step_time, start_process_time, '')

        utl_create_source2parquet_log_entry(guid, process_name, '', 'general_step',
                                            'success', datetime.now(), start_process_time, start_process_time, '')

        logger.info("************* End Job *************")

    except Exception as e:
        logger.error(e)
        utl_create_source2parquet_log_entry(guid, process_name, '', 'general_step', 'failure',
                                            datetime.now(), start_process_time, start_process_time, str(e))
        raise



