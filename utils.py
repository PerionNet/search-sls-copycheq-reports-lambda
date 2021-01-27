import pymysql
import os
import logging
import requests
import json
import boto3
import botocore

# Lambda ENVIRONMENT VARIABLES
rds_host = os.environ['RDS_HOST']
username = os.environ['DB_USERNAME']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def utl_create_source2parquet_log_entry(guid, process_name, sub_process_name, step_name, step_status, insert_time,
                                        start_step_time, start_process_time, err_msg):
    """
    Insert record to source2parquet log table.

        Args:
            guid: process id.
            process_name: process name.
            sub_process_name: sub process name.
            step_name: step name.
            step_status: step status.
            insert_time: insert log time.
            start_step_time: time that step started.
            start_process_time: time that process started.
            err_msg: error message.

        Raises:
            Exception: mysql error.
    """
    try:
        env_param = os.environ['ENV']
        config = read_config(env_param)
        mysql_conn = pymysql.connect(host=rds_host,
                                     user=username,
                                     passwd=password,
                                     db=db_name)

        sp_name = "sp_utl_create_source2parquet_log_entry"
        sp_args = (
        guid, process_name, sub_process_name, step_name, step_status,
        insert_time, start_step_time, start_process_time, err_msg)
        curs = mysql_conn.cursor()

        curs.callproc(sp_name, sp_args)
        mysql_conn.commit()
        # print curs.fetchone()

    except Exception as e:
        print(e)
        raise
    finally:
        mysql_conn.close()  # Use all the SQL you like


def copy_s3_file(source_bucket, source_key, destination_bucket, destination_path):
    s3_source_bucket = source_bucket
    s3_source_key = source_key
    s3_destination_bucket = destination_bucket
    s3_destination_path = destination_path

    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': s3_source_bucket,
        'Key': s3_source_key
    }
    s3.meta.client.copy(copy_source, s3_destination_bucket, s3_destination_path)
    print('file copy from {fro} to {to}'.format(fro=s3_source_bucket + '/' + s3_source_key,
                                                to=s3_destination_bucket + '/' + s3_destination_path))


