from datetime import datetime
import logging

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator, DagBag
# from airflow.operators.dagrun_operator import DagRunOrder
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow import settings


class ExecuteDagRunOperator(BaseOperator):
    """
    Execute's an external DAG with the same execution_date as the triggering dag
    :param execute_dag_id: the dag_id to execute
    :type execute_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    """
    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#c2cfef'

    #@apply_defaults
    def __init__(
            self,
            execute_dag_id,
            python_callable,
            *args, **kwargs):
        super(ExecuteDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.execute_dag_id = execute_dag_id

    def execute(self, context):
        dro = TriggerDagRunOperator(run_id='exec__' + datetime.utcnow().isoformat())
        dro = self.python_callable(context, dro)
        if dro:
            session = settings.Session()
            dbag = DagBag(settings.DAGS_FOLDER)
            execute_dag = dbag.get_dag(self.execute_dag_id)
            dr = execute_dag.create_dagrun(
                run_id=dro.run_id,
                state=State.RUNNING,
                execution_date=context['execution_date'],
                conf=dro.payload,
                external_trigger=True)
            logging.info("Creating DagRun %s", dr)
            session.add(dr)
            session.commit()
            session.close()
        else:
            logging.info("Criteria not met, moving on")
