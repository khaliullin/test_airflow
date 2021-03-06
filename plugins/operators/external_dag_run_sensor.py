from airflow.plugins_manager import AirflowPlugin
from airflow import DAG, settings
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import timedelta, datetime
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults
from airflow.models import DagRun
import logging


class ExternalDagRunSensor(BaseSensorOperator):
    """
    Waits for different DAG run to complete
    :param external_dag_id: The dag_id that you want to wait for
    :type external_dag_id: string
    :param allowed_states: list of allowed states, default is ``['Running']``
    :type allowed_states: list
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same execution_date as the current task.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :type execution_delta: datetime.timedelta
    :param execution_date_fn: function that receives the current execution date
        and returns the desired execution date to query. Either execution_delta
        or execution_date_fn can be passed to ExternalTaskSensor, but not both.
    :type execution_date_fn: callable
    """

    ui_color = '#c2cfef'

    #@apply_defaults
    def __init__(
            self,
            external_dag_id,
            allowed_states=None,
            execution_delta=None,
            execution_date_fn=None,
            *args, **kwargs):
        super(ExternalDagRunSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.RUNNING]
        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                'Only one of `execution_date` or `execution_date_fn` may'
                'be provided to ExternalDagRunSensor; not both.')

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_id = external_dag_id

    def poke(self, context):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self.execution_date_fn(context['execution_date'])
        else:
            dttm = context['execution_date']

        logging.info(
            'Poking for '
            '{self.external_dag_id} on '
            '{dttm} ... '.format(**locals()))

        session = settings.Session()
        count = session.query(DagRun).filter(
            DagRun.dag_id == self.external_dag_id,
            DagRun.state.in_(self.allowed_states),
            DagRun.execution_date <= dttm,
        ).count()
        session.commit()
        session.close()
        if count == 0:
            return True
        else:
            return False
