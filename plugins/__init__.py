from airflow.plugins_manager import AirflowPlugin
from plugins.hooks.sentry_hook import SentryHook
from plugins.operators.external_dag_run_sensor import ExternalDagRunSensor
from plugins.operators.s3_to_redshift_operator import S3ToRedshiftOperator
from plugins.operators.execute_dag_run_operator import ExecuteDagRunOperator
from plugins.operators.api_to_redshift_operator import ApiToRedshiftOperator

#  @aat comment 
#  class SentryPlugin(AirflowPlugin):
#    name = "SentryPlugin"
#    hooks = [SentryHook]


class ExternalDagRunSensorPlugin(AirflowPlugin):
    name = "ExternalDagRunSensorPlugin"
    operators = [ExternalDagRunSensor]


class S3ToRedshiftOperatorPlugin(AirflowPlugin):
    name = "S3ToRedshiftOperatorPlugin"
    operators = [S3ToRedshiftOperator]


class ExecuteDagRunOperatorPlugin(AirflowPlugin):
    name = "ExecuteDagRunOperatorPlugin"
    operators = [ExecuteDagRunOperator]


class ApiToRedshiftOperatorPlugin(AirflowPlugin):
    name = "ApiToRedshiftOperatorPlugin"
    operators = [ApiToRedshiftOperator]
