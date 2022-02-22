import os
import subprocess
import logging
import datetime
import time
import boto3
import requests
import simplejson as json
import pandas as pd
from plugins.operators.api_to_redshift_operator import ApiToRedshiftOperator
from plugins.operators.s3_to_redshift_operator import S3ToRedshiftOperator
from airflow.operators.subdag import SubDagOperator
import plugins.redshift as redshift


def _failure_notification(context):
    # do nothing yet here
    print(context)
    return True


log = logging.getLogger("logger")


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['data.integration@awaytravel.com'], 
    'provide_context': True,
    'catchup': False,
    'start_date': datetime.datetime(2018, 1, 1), 
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
