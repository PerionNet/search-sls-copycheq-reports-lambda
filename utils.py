import pymysql
import os
import logging
import requests
import json
import boto3
import botocore
import time
boto3.set_stream_logger('', logging.INFO)

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
        logger.error(e)
        raise e
    finally:
        mysql_conn.close()  # Use all the SQL you like


def copy_s3_file(source_bucket, source_key, destination_bucket, destination_path):
    # Try 3 attempts to copy file
    attempts = 0
    while attempts < 3:
        try:
            s3_source_bucket = source_bucket
            s3_source_key = source_key
            s3_destination_bucket = destination_bucket
            s3_destination_path = destination_path

            s3 = boto3.resource('s3')
            copy_source = {
                'Bucket': s3_source_bucket,
                'Key': s3_source_key
            }
            s3.meta.client.copy(copy_source, s3_destination_bucket, s3_destination_path,
                                ExtraArgs={'ACL': 'bucket-owner-full-control'})

            logger.info('file copy from {fro} to {to}'.format(fro=s3_source_bucket + '/' + s3_source_key,
                                                              to=s3_destination_bucket + '/' + s3_destination_path))
            # Copy successfully
            attempts = 3
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "403":
                logger.error("HeadObject operation: Forbidden - error code 403")
                logger.info('copy_s3_file failed with code 403')
                logger.info('Error Message: {}'.format(e.response['Error']['Message']))
                logger.info('Request ID: {}'.format(e.response['ResponseMetadata']['RequestId']))
                logger.info('Http code: {}'.format(e.response['ResponseMetadata']['HTTPStatusCode']))

                attempts += 1
                if attempts == 3:
                    raise ValueError("Copy failed after 3 attempts")
                time.sleep(20)
                pass
            else:
                logger.error(e)
                raise e

        except Exception as e:
            logger.error(e)
            raise e



