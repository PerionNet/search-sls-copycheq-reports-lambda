import pymysql
import os
import logging
import requests
import json
import boto3
import botocore

# Lambda ENVIRONMENT VARIABLES
rds_host = os.environ['RDS_HOST']
af_url = os.environ['AIRFLOW_URL']
username = os.environ['DB_USERNAME']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']
source_file_aws_lambda_trigger_tbl = os.environ['SOURCE_FILS_AWS_LAMBDA_TRIGGER_TBL']
source_file_audit_tbl = os.environ['SOURCE_FILE_AUDIT_TBL']
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
        mysql_conn = pymysql.connect(host=config["mysql"]["host"],
                               user=config["mysql"]["user"],
                               passwd=config["mysql"]["password"],
                               db=config["mysql"]["database"])

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


def insert_file_lambda_trigger(source_file, file_status, file_date, file_path, file_size):
    """ This function fetches object content to MySQL table source_file_aws_lambda_trigger """
    try:
        conn = pymysql.connect(rds_host, user=username, passwd=password, db=db_name, connect_timeout=5,
                               autocommit=False)
        cur = conn.cursor()
        logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

        mysql_query = """INSERT INTO source_file_aws_lambda_trigger(source_file,
                                                                    process_status,
                                                                    file_date,
                                                                    s3_file_path,
                                                                    file_size_mb)
                         VALUES('{source_file}','{process_status}','{file_date}','{s3_file_path}',{file_size_mb})"""\
            .format(source_file=source_file,
                    process_status=file_status,
                    file_date=file_date,
                    s3_file_path=file_path,
                    file_size_mb=file_size)

        logger.info(mysql_query)

        cur.execute(mysql_query)
        conn.commit()
        logger.info("SUCCESS: Insert Record to RDS MySQL instance succeeded")
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        raise e
    finally:
        conn.close()


def trigger_dag(dag_id, guid_id):
    """ This function triggers a dag in airflow """
    try:
        headers = {'Cache-Control': "no-cache", 'Content-Type': "application/json"}
        data_key = dag_id + '_guid_id'
        data_value = guid_id
        data = {
            "conf": {
                data_key: data_value
            }
        }

        url = af_url + "/api/experimental/dags/{dag_id}/dag_runs".format(dag_id=dag_id)
        logger.info(url)

        # check if the dag is already running
        response = requests.get(url, data=json.dumps(data), headers=headers)
        if response.status_code == 200:
            logger.info("SUCCESS: Get Dag Run")
            # no records for previous dag runs
            if not response.json():
                logger.info("No records for previous dag runs, set dag state to new")
                dag_state = 'new'
            # there are previous records
            else:
                # get last dag id state - Enum: 'success', 'running', 'failed'
                dag_state = response.json()[-1]['state']
                txt_info = "The state of the last dag run is: {}".format(dag_state)
                logger.info(txt_info)
        else:
            err_msg = "ERROR: Get Dag Run Failed. Response Status Code is: {}".format(response.status_code)
            logger.error(err_msg)
            raise ValueError(err_msg)

        # dag is not running, trigger dag
        if dag_state != 'running':
            response = requests.post(url, data=json.dumps(data), headers=headers)

            if response.status_code == 200:
                logger.info("SUCCESS: Dag Trigger")
            else:
                err_msg = "ERROR: Dag Trigger Failed. Response Status Code is: {}".format(response.status_code)
                logger.error(err_msg)
                raise ValueError(err_msg)
        else:
            logger.info("No Trigger, Dag Already Running")

    except Exception as e:
        logger.error(e)
        raise e
