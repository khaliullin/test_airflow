import os
import requests
import traceback
import sys
import opsgenie_sdk 
from opsgenie_sdk import ApiException

'''
#############################################
## failure_notifications ##
## added Andrei A. Toropov
## version 0.1 for Airflow-2
## started 10/26/2021
## pytest --tb=line 
#############################################
 
'''

class Opsgenie:

        def __init__(self):
            self.opsgenie_api_key = os.environ['OPSGENIE_KEY'] 
            if self.opsgenie_api_key == None: 
                print("Exception when read OPSGENIE_KEY:")
                return
            self.conf = self.conf = opsgenie_sdk.configuration.Configuration()
            self.conf.api_key['Authorization'] = self.opsgenie_api_key

            self.api_client = opsgenie_sdk.api_client.ApiClient(configuration=self.conf)
            self.alert_api = opsgenie_sdk.AlertApi(api_client=self.api_client)
 
        def send_notification(self, context, doSend=True):  

            payload = opsgenie_sdk.CreateAlertPayload(
                message="DAG: {}".format(context['task_instance'].dag_id),
                alias='Airflow-2',
                description="*Task:* {}\n\n :skull:\n\n *Error:* {}".format(context['task_instance'].task_id, context['exception']),
                responders=[{
                    'name': 'Data Engineering',
                    'type': 'team'
                }],
                visible_to=[{'name': 'Data Engineering', 'type': 'team'}],
                actions=['Log', 'DAG'],
                tags=['Airflow-2'], 
                entity="https://away-airflow-2.herokuapp.com/tree?dag_id={}".format(context['task_instance'].dag_id),
                priority='P3'
            )

            if doSend == True:   
                try:
                    create_response = self.alert_api.create_alert(create_alert_payload=payload)
                    # print(create_response)
                    return create_response
                except opsgenie_sdk.ApiException as err:
                    print("Exception when calling AlertApi->create_alert: %s\n" % err)
            else:
                print("::: Not not send :::")

def opsgenie_notification(context,doSend=True):
    o = Opsgenie()
    if doSend: 
        o.send_notification(context, doSend)
        return doSend
    else:
        return True

def failure_notification(context,doSend=True):
    
    try:
        # do opsgenie_notification
        opsgenie_notification(context,doSend)
    except Exception as e: 
        traceback.print_exc(file=sys.stdout)
    
    payload = [
        {
            "color": "danger"
            ,"title": "DAG: {}".format(context['task_instance'].dag_id)
            ,"title_link": "https://away-airflow-2.herokuapp.com/tree?dag_id={}".format(context['task_instance'].dag_id)
            ,"text": "*Task:* {}\n\n :skull:\n\n *Error:* {}".format(context['task_instance'].task_id, context['exception'])
        }
    ]

    try:
        if doSend: 
            requests.post(os.environ['SLACK_ERROR_CALLBACK_HOOK'], json = {"attachments": payload})
            return doSend
        else:
            return True
    except Exception as e: 
        traceback.print_exc(file=sys.stdout)

 
    print("<<< success_notification >>>")
    payload = [
        {
            "color": "danger"
            ,"title": "DAG: {}".format(context['task_instance'].dag_id)
            ,"title_link": "https://away-airflow-production.herokuapp.com/tree?dag_id={}".format(context['task_instance'].dag_id)
            ,"text": "*Task:* {}\n\n :skull:\n\n *Error:* {}".format(context['task_instance'].task_id, context['exception'])
        }
    ] 
    if doSend: 
        # requests.post(os.environ['SLACK_ERROR_CALLBACK_HOOK'], json = {"attachments": payload})
        return doSend
    else:
        return True