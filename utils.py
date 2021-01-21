import pymysql
import os
import logging
import requests
import json


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


def utl_create_filesproviders_log_entry(guid, process_name, sub_process_name, step_name, step_status, insert_time,
                                        start_step_time, start_process_time, err_msg):
    """
    Insert record to filesproviders log table.

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

        sp_name = "sp_utl_create_filesproviders_log_entry"
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
        mysql_conn.close()


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


def num_files_to_process_filesproviders(source_file):
    files_to_process = 0

    try:
        # get pending files to process - returned value is [id, file_date, s3_file_path]
        pending_files = get_pending_files_lambda_trigger(source_file)

        for pending_file in pending_files:
            row_id = pending_file[0]
            file_date = pending_file[1]
            s3_file_path = pending_file[2]
            # get the file name we are working on
            file_name = s3_file_path.split('/')[-1]

            # get the last file processed for the date and source_file or 'default' value for first time process
            last_file_processed = get_last_file_processed_audit(source_file, file_date)
            if file_name >= last_file_processed or last_file_processed == 'default':
                # file to process it bigger or equal to last file, can proceed with process
                files_to_process += 1
                # update file to READY
                logger.info("New file to process, Update Pending file to READY")
                update_pending_file_to_new_status(row_id, 'READY')
            else:
                # File already processed, update file to DONE
                logger.info("File already processed, Update Pending file to DONE")
                update_pending_file_to_new_status(row_id, 'DONE')

        return files_to_process

    except Exception as e:
        logger.error(e)
        raise e


def get_pending_files_lambda_trigger(source_file):
    """
        Get records from source_file_aws_lambda_trigger with status PENDING.

            Args:
                source_file: source file to check for.

            Raises:
                Exception: mysql error.
        """
    try:
        mysql_conn = pymysql.connect(host=rds_host,
                                     user=username,
                                     passwd=password,
                                     db=db_name)
        # prepare a cursor object using cursor() method
        cursor = mysql_conn.cursor()

        mysql_query = """SELECT id,
                              file_date,
                              s3_file_path
                        FROM {}
                        WHERE process_status= 'PENDING'
                        and source_file='{}';""".format(source_file_aws_lambda_trigger_tbl,
                                                        source_file)

        logger.info(mysql_query)

        # execute SQL query using execute() method.
        cursor.execute(mysql_query)
        pending_files = cursor.fetchall()

        return pending_files

    except Exception as e:
        logger.error(e)
        raise e
    finally:
        mysql_conn.close()


def get_last_file_processed_audit(source_file, dt):
    """
        Get records from source_file_aws_lambda_trigger with status pending.

            Args:
                source_file: source file to check for.
                dt: date to check.

            Raises:
                Exception: mysql error.
        """
    try:
        mysql_conn = pymysql.connect(host=rds_host,
                                     user=username,
                                     passwd=password,
                                     db=db_name)
        # prepare a cursor object using cursor() method
        cursor = mysql_conn.cursor()

        mysql_query = """SELECT last_file_processed
                       FROM {}
                       WHERE dt='{}' and source_file='{}';""".format(source_file_audit_tbl,
                                                                     dt,
                                                                     source_file)
        logger.info(mysql_query)

        # execute SQL query using execute() method.
        cursor.execute(mysql_query)
        last_file_processed = cursor.fetchone()

        if last_file_processed:
            # There is a value in audit table, source file was process once
            return last_file_processed[0]
        else:
            # There is no value in audit table, source file was never processed
            return 'default'

    except Exception as e:
        logger.error(e)
        raise e
    finally:
        mysql_conn.close()


def update_pending_file_to_new_status(row_id, status):
    """
        update record in source_file_aws_lambda_trigger to status READY OR DONE.

            Args:
                row_id: row id to update ub .
                    dt: date to check.

                Raises:
                    Exception: mysql error.
            """
    try:
        mysql_conn = pymysql.connect(host=rds_host,
                                     user=username,
                                     passwd=password,
                                     db=db_name)
        # prepare a cursor object using cursor() method
        cursor = mysql_conn.cursor()

        mysql_query = """UPDATE {}
                       SET process_status='{}',
                           update_time=CURRENT_TIMESTAMP
                       WHERE id={} and process_status='PENDING';"""\
            .format(source_file_aws_lambda_trigger_tbl, status, row_id)
        logger.info(mysql_query)

        # execute SQL query using execute() method.
        cursor.execute(mysql_query)
        mysql_conn.commit()

    except Exception as e:
        logger.error(e)
        raise e
    finally:
        mysql_conn.close()


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
