import os
import requests
def success_notification(context,doSend=True):
    # print("<<< success_notification >>>", context)
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/taskinstance/index.html
    #print("<<< success_notification >>>", context['task_instance'])
    #print("<<< success_notification >>>", context['task_instance'].dag_id)
    #print("<<< success_notification >>>", context['task_instance'].task_id)
    #print("<<< success_notification >>>", context['task_instance'].state)
  
    payload = [
        {
            "color": "success"
            ,"title": "DAG: {}".format(context['task_instance'].dag_id)
            ,"title_link": "https://away-airflow-2-production.herokuapp.com/tree?dag_id={}".format(context['task_instance'].dag_id)
            ,"text": "*Task:* {}\n\n :skull:\n\n *Success:* {}".format(context['task_instance'].task_id, context['task_instance'].state)
        }
    ] 
 
    # print("<<< success_notification >>>", payload)

    if doSend: 
        # requests.post(os.environ['SLACK_ERROR_CALLBACK_HOOK'], json = {"attachments": payload})
        return doSend
    else:
        return True
