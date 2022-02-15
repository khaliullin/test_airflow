# airflow
import datetime
import json
import os
import sys
import requests
from time import sleep
from colorama import Fore, Style

import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from urllib.parse import urlencode, urlunparse
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from dags import DEFAULT_ARGS

with DAG(
        dag_id='tiktok',
        description='Get ads data from TikTok Marketing API',
        default_args=DEFAULT_ARGS,
        schedule_interval='0 6 * * *',
        start_date=days_ago(3),
        tags=['tiktok'],
) as dag:
    ACCESS_TOKEN = os.environ['TIKTOK_ACCESS_TOKEN']
    ADVERTISER_ID = os.environ['TIKTOK_ADVERTISER_ID']
    app_id = os.environ['TIKTOK_APPID']
    secret = os.environ['TIKTOK_SECRET']

    db_string = "postgresql://awaytravel:Awaytravel1000!@awaytravel-staging-postgres.cz7zyrgalx0j.us-east-1.rds.amazonaws.com:5432/airflow-2"
    db = create_engine(db_string)

    DATE_END = datetime.date.today()
    DAYS = 3

    date_start = DATE_END - datetime.timedelta(days=DAYS)
    start_date = date_start.isoformat()
    end_date = DATE_END.isoformat()

    ids_cache = dict()

    tasks = [
        # https://ads.tiktok.com/marketing_api/docs?id=1708582970809346
        {
            "title": "Get campaigns", "table_name": "campaigns", "path": "/campaign/get/",
            "args": {
                "advertiser_id": f"{ADVERTISER_ID}", "page_size": "1000"
            },
            "name": "campaign_name", "id": "campaign_id",
            "cache_name": "campaign_ids"
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1708572923161602
        {
            'title': 'Ads Managment', 'path': '/ad/get/', 'table_name': 'ads_managment',
            "id": "ad_id",
            "cache_name": "ad_ids",
            "args": {
                'advertiser_id': f'{ADVERTISER_ID}', 'page_size': "1000"
            },
            'children': [
                # https://ads.tiktok.com/marketing_api/docs?id=1701890938168321
                {'path': '/ad/review_info/', 'ref': 'ad_id', 'param': 'ad_ids', 'mapname': 'ad_review_map',
                 'child_id': 'id'},
                # https://ads.tiktok.com/marketing_api/docs?id=1709502220585986
                {'path': '/ad/dynamic/get/', 'ref': 'adgroup_id', 'param': 'adgroup_ids',
                 'mapname': 'ad_dynamic_map', 'child_id': 'id'},
                # https://ads.tiktok.com/marketing_api/docs?id=1714193310609409
                {'path': '/ad/aco/get/', 'ref': 'adgroup_id', 'param': 'adgroup_ids', 'mapname': 'ad_group_aco_map',
                 'child_id': 'id'},
            ]
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1708503489590273
        {
            "title": "Ads Group Managment", "path": "/adgroup/get/", "table_name": "adgroup",
            "id": "adgroup_id",
            "cache_name": "adgroup_ids",
            "args": {
                "advertiser_id": f"{ADVERTISER_ID}", "page_size": "1000"
            },
            "children": [
                # https://ads.tiktok.com/marketing_api/docs?id=1701890933736450
                {
                    "path": "/adgroup/review_info/", "ref": "adgroup_id",
                    "param": "adgroup_ids", "mapname": "ad_group_review_map", 'child_id': 'id'
                }
            ]
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1701890967858177
        {
            'title': 'Get Creative Report', 'table_name': 'creative_report', 'path': '/creative/reports/get/',
            'id': 'info_material_id',
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'material_type': 'VIDEO',
                'lifetime': True,
                'page_size': '1000'
            }
        },
        {
            'title': 'Get Creative Report', 'table_name': 'creative_report2', 'path': '/creative/reports/get/',
            'id': 'info_material_id',
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'material_type': 'IMAGE',
                'lifetime': True,
                'page_size': '1000'
            }
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1709497028894721
        {
            'title': 'Get pixel', 'table_name': 'pixel', 'path': '/pixel/list/', 'list': 'pixels',
            'id': 'pixel_id', "child_id": "pixel_id",
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'page_size': '20'
            },
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1709497606974466
        {
            'title': 'Get event statistics', 'path': '/pixel/event/stats/', 'list': 'list',
            'table_name': 'pixel_event_stats',
            'id': 'pixel_id',
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'date_range': {
                    'start_date': start_date,
                    'end_date': end_date,
                },
                'pixel_ids': [
                    '6987488643342467073',
                    '6983050233483739138',
                ]
            }
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1708882535079937
        {
            'title': 'Get comments', 'path': '/comment/list/',
            'table_name': 'comments',
            'id': 'comments_id',
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'search_field': 'ADGROUP_ID',
                'search_value': '%',
                'start_time': start_date,
                'end_time': end_date,
                'page_size': '100'
            }
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1711590038616066
        {
            'title': 'Get stores', 'table_name': 'stores', 'path': '/commerce/store/get/', "list": "stores",
            'id': 'stores_id',
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1701890940791810
        {
            'title': 'Get test results', 'path': '/split_test/result/get/',
            'table_name': 'split_test_results', 'name': 'name', 'id': 'campaign_id'
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1708886607776770
        {
            'title': 'Get blocked words', 'path': '/blockedword/list/',
            'table_name': 'blocked_words',
            'id': 'blocked_words_id',
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'page_size': '500'
            }
        },
        # https://ads.tiktok.com/marketing_api/docs?id=1701890954187778
        {
            'title': 'Ad Campaign Audience Report',
            'path': '/audience/campaign/get/',
            'table_name': 'audience_campaign_report',
            "id": "dimensions_campaign_id",
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'campaign_ids': "",
                'dimensions': [
                    # 'GENDER',
                    # 'AGE',
                    # 'COUNTRY',
                    # 'AC',
                    'LANGUAGE',
                    # 'PLATFORM',
                    # 'INTEREST_CATEGORY',
                    # 'PLACEMENT',
                ],
                'start_date': start_date,
                'end_date': end_date,
                'fields': ['show_cnt', 'stat_cost']
            }
        },
        {
            'title': 'Ad Group Audience Report',
            'path': '/audience/adgroup/get/',
            'table_name': 'audience_adgroup_report',
            'id': 'dimensions_adgroup_id',
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'adgroup_ids': "",
                'dimensions': [
                    # 'GENDER',
                    # 'AGE',
                    # 'COUNTRY',
                    # 'AC',
                    'LANGUAGE',
                    # 'PLATFORM',
                    # 'INTEREST_CATEGORY',
                    # 'PLACEMENT',
                ],
                'start_date': start_date,
                'end_date': end_date,
                'fields': ['show_cnt', 'stat_cost']
            }
        },
        {
            'title': 'Ad Audience Report',
            'path': '/audience/ad/get/',
            'table_name': 'audience_ad_report',
            'id': 'dimensions_ad_id',
            'args': {
                'advertiser_id': f'{ADVERTISER_ID}',
                'ad_ids': "",
                'dimensions': [
                    # 'GENDER',
                    # 'AGE',
                    # 'COUNTRY',
                    # 'AC',
                    'LANGUAGE',
                    # 'PLATFORM',
                    # 'INTEREST_CATEGORY',
                    # 'PLACEMENT',
                ],
                'start_date': start_date,
                'end_date': end_date,
                'fields': ['show_cnt', 'stat_cost']
            }
        },
    ]


    def build_url(path, query=""):
        # type: (str, str) -> str
        """
        Build request URL
        :param path: Request path
        :param query: Querystring
        :return: Request URL
        """
        scheme, netloc = "https", "business-api.tiktok.com"
        return urlunparse((scheme, netloc, path, "", query, ""))


    def post(path, json_str):
        # type: (str, str) -> dict
        # PATH = "/open_api/v1.2/oauth2/access_token/"
        """
        Send POST request
        :param json_str: Args in JSON format
        :return: Response in JSON format
        """
        url = build_url(path)
        args = json.loads(json_str)
        headers = {
            "Content-Type": "application/json",
            "Access-Token": ACCESS_TOKEN
        }
        response = requests.post(url, headers=headers, json=args)
        print("<< response >> ", response)
        return response.json()


    def get_auth_code():
        auth_code = os.environ['TIKTOK_AUTHCODE']
        state = "AWAYTOKEN"
        response = requests.get(
            url=f'https://ads.tiktok.com/marketing_api/auth?app_id=${app_id}&state=${state}&'
                f'redirect_uri=https%3A%2F%2Faway-airflow-2.herokuapp.com%2Fapi%2Fv1%2FtiktokCallback&rid=6610w2ge2em',
        )
        print(response)
        return auth_code


    def access_token_request(json_str):
        # type: (str) -> dict
        path = "/open_api/v1.2/oauth2/access_token/"
        """
        Send POST request
        :param json_str: Args in JSON format
        :return: Response in JSON format
        """
        url = build_url(path)
        args = json.loads(json_str)
        headers = {
            "Content-Type": "application/json"
        }
        rsp = requests.post(url, headers=headers, json=args)
        return rsp.json()


    def get(path, json_str):
        # type: (str, str) -> dict
        """
        Send GET request
        :param json_str: Args in JSON format
        :return: Response in JSON format
        """
        args = json.loads(json_str)
        query_string = urlencode(
            {k: v if isinstance(v, str) else json.dumps(v) for k, v in args.items()}
        )
        url = build_url(path, query_string)
        headers = {
            "Content-Type": "application/json",
            "Access-Token": ACCESS_TOKEN
        }
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            return response
        else:
            return response.json()


    def campaign(path):
        path = f'/open_api/v1.2{path}'
        # /campaign/get/

        # fields_list = FIELDS
        # fields = json.dumps(fields_list)
        # status = STATUS
        # campaign_name = CAMPAIGN_NAME
        # objective_type = OBJECTIVE_TYPE
        # campaign_ids_list = CAMPAIGN_IDS
        # campaign_ids = json.dumps(campaign_ids_list)
        # primary_status = PRIMARY_STATUS
        # page = PAGE
        # page_size = PAGE_SIZE

        # Args in JSON format
        # my_args = "{\"advertiser_id\": \"%s\", \"fields\": %s, \"filtering\": {\"status\": \"%s\", \"campaign_name\": \"%s\", \"objective_type\": \"%s\", \"campaign_ids\": %s, \"primary_status\": \"%s\"}, \"page\": \"%s\", \"page_size\": \"%s\"}" % (advertiser_id, fields, status, campaign_name, objective_type, campaign_ids, primary_status, page, page_size)

        args = "{\"advertiser_id\": \"%s\", \"filtering\":{}}" % (ADVERTISER_ID)

        # print(get(path, args))
        return get(path, args)


    def get_access_token(auth_code):
        access_args = "{\"secret\": \"%s\", \"app_id\": \"%s\", \"auth_code\": \"%s\"}" % (secret, app_id, auth_code)
        response = access_token_request(access_args)
        message = response["message"]
        if response["code"] == "0" and response["data"] and response["data"]["access_token"]:
            return response["data"]["access_token"]
        else:
            print('Warning message: %s' % message)
            print(response)
            return ""


    def generate_access_token():
        auth_code = ""
        '''
        auth_code from API TikTok 
        example: 
        auth_code = "cc3c5ad09ac4e05ba1876c5b318ebb1f4cd42007"
        '''

        try:
            access_token = get_access_token(auth_code)
            if access_token:
                os.environ["TIKTOK_ACCESS_TOKEN"] = access_token
        except Exception as e:
            exc_tb = sys.exc_info()
            success = False
            os.environ["TIKTOK_ACCESS_TOKEN"] = ""
            print('An error occurred: %s' % e, "at line:", exc_tb.tb_lineno)

            '''
            {'message': 'OK', 'code': 0, 'data': {'access_token': 'af102be01404a4ee193faa96bc0618b90baf0214', 'scope': [19000000, 65, 800, 802, 15020100, 4, 960, 97, 200, 900, 910, 210, 51, 15010100, 220, 95], 'advertiser_ids': [6944022146033188865, 7029003159712350209]}, 'request_id': '20211110182835010245244247207772AE'}
            '''


    def get_data(path, args):
        path = f'/open_api/v1.2{path}'
        if args is None:
            args = '{"advertiser_id": "%s", "filtering":{}}' % (ADVERTISER_ID)
        return get(path, args)


    def post_data(path, args):
        path = f'/open_api/v1.2{path}'
        if args is None:
            args = '{"advertiser_id": "%s", "filtering":{}}' % (ADVERTISER_ID)
        return post(path, args)


    def post_data_v1(path, args):
        path = f'/open_api/v1.1{path}'
        if args is None:
            args = '{"advertiser_id": "%s", "filtering":{}}' % (ADVERTISER_ID)
        return post(path, args)


    def get_content(path, json_str):
        # type: (str, str) -> dict
        path = f'/open_api/v1.2{path}'
        """
        Send GET request
        :param json_str: Args in JSON format
        :return: Response in JSON format
        """
        args = json.loads(json_str)
        query_string = urlencode(
            {k: v if isinstance(v, str) else json.dumps(v) for k, v in args.items()}
        )
        url = build_url(path, query_string)
        headers = {
            "Access-Token": ACCESS_TOKEN,
        }
        response = requests.get(url, headers=headers)
        content = response.content
        return content


    def db_action(input_object, schema_name: str, table_name: str, primary_key_name: str):
        df = pd.json_normalize(input_object, sep="_")
        df.set_index(primary_key_name)
        df['load_date'] = pd.to_datetime('now')
        df['source'] = "Tiktok AD"
        df = df.drop(['events', 'action_v2', 'pixels', 'statistics'], axis=1, errors='ignore')
        df.to_sql(table_name, con=db, schema=schema_name, if_exists='replace', index=False)

        '''
        see example here:
        drop_table("reports")
        _ds = pd.DataFrame(data=reports,
                        columns=["name",
                                    "description", "language",
                                    "id", "created"])
        RedshiftLoad(data=_ds, schema=schema,
                    table='reports',
                    unique_key='key')
        '''

        return df


    def create_report():
        # report_type = "BASIC"
        # data_level = "AUCTION_CAMPAIGN"
        # lifetime = "true"
        # dimensions = """["campaign_id"]"""

        report_type = "BASIC"
        data_level = "AUCTION_CAMPAIGN"
        lifetime = "false"
        # "YYYY-MM-DD"
        start_date = "2021-10-15"
        end_date = "2021-11-15"
        dimensions = """["campaign_id"]"""

        # https://ads.tiktok.com/marketing_api/docs?id=1707957217727489

        ##
        ## Audience report
        #

        # 1. Query the overall age distribution of the audience within a period of time, sorted in ascending order of spend
        report_type = "AUDIENCE"
        data_level = "AUCTION_ADVERTISER"
        service_type = "AUCTION"
        dimensions = """["age"]"""

        # 2. Query the age and gender distribution of all audiences in the life cycle of an advertiser
        report_type = "AUDIENCE"
        data_level = "AUCTION_ADVERTISER"
        service_type = "AUCTION"
        dimensions = """["age","gender"]"""

        # 3. Query the country distribution of all undeleted ad groups under certain campaigns over a period of time
        report_type = "AUDIENCE"
        data_level = "AUCTION_ADGROUP"
        service_type = "AUCTION"
        dimensions = """["country_id","adgroup_id"]"""

        args = f'''{{
            "advertiser_id": "{ADVERTISER_ID}",
            "service_type": "{service_type}",
            "report_type": "{report_type}",
            "data_level": "{data_level}",
            "lifetime": {lifetime},
            "dimensions": {dimensions},
            "start_date": "{start_date}",
            "end_date": "{end_date}"
            }}
            '''
        print(args)
        response_data = post_data("/reports/integrated/get/", args)
        print(response_data)

        task_id = response_data.get("data", {}).get("task_id")
        if task_id:
            # https://business-api.tiktok.com/open_api/v1.2/reports/task/check/
            args = f'{{"task_id": "{task_id}"}}'
            timer = 0
            timewait = 2
            timeout = 60
            g = get_data("/reports/task/check/", args)
            while g["data"]["status"] == "QUEUING" or g["data"]["status"] == "PROCESSING":
                sleep(timewait)
                g = get_data("/reports/task/check/", args)
                timer += timewait
                if timer > timeout:
                    break
                if g["data"]["status"] == "SUCCESS":
                    break
                print("Waiting:", timer, g["data"]["status"])

            print(task_id, ":", g)
            # https://business-api.tiktok.com/open_api/v1.2/reports/task/download/
            content = get_content("/reports/task/download/", args)
            content = content.decode("utf-8")
            print(content)


    def blockedword_task():
        create = '/blockedword/task/create/'
        check = '/blockedword/task/check/'
        download = '/blockedword/task/download/'

        request_args = "{\"advertiser_id\": \"%s\"}" % (ADVERTISER_ID)

        response_data = post_data(create, request_args)
        print(response_data)

        task_id = response_data.get("data", {}).get("task_id")
        if task_id:
            args = f'{{"task_id": "{task_id}", "advertiser_id": "{ADVERTISER_ID}"}}'
            timer = 0
            timewait = 2
            timeout = 60
            check_response = get_data(check, args)
            while check_response["data"]["status"] == "QUEUING" or check_response["data"]["status"] == "PROCESSING":
                sleep(timewait)
                check_response = get_data(check, args)
                timer += timewait
                if timer > timeout:
                    break
                if check_response["data"]["status"] == "SUCCESS":
                    break
                print("Waiting:", timer, check_response["data"]["status"])

            print(task_id, ":", check_response)
            # https://business-api.tiktok.com/open_api/v1.2/reports/task/download/
            content = get_content(download, args)
            content = content.decode("utf-8")
            print(content)


    def execute_tasks(tasks):
        for t in tasks:
            path = t['path']
            title = t['title']
            table_name = t.get('table_name', '')
            primary_key = t.get('id', '')
            list_name = t.get('list', 'list')
            args = t.get("args")
            if args:
                empty_args = [(arg_key, arg_val) for arg_key, arg_val in args.items() if arg_val == ""]
                for arg_key, arg_val in empty_args:
                    args[arg_key] = ids_cache.get(arg_key, [])[:100]
                args = json.dumps(t['args'])

            d = get_data(path, args)

            if "data" in d and bool(d['data']) and len(d["data"][list_name]) > 0:
                # reading lists array name from props
                try:
                    data_df = db_action(
                        input_object=d["data"][list_name],
                        schema_name="tiktok",
                        table_name=table_name,
                        primary_key_name=primary_key
                    )
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    message = 'An error occurred: %s' % e
                    print(Fore.RED + message + Style.RESET_ALL, "at line:", exc_tb.tb_lineno, "\n", d)
                    continue

                # Add primary keys to response if cache_name is defined.
                ids_cache_name = t.get("cache_name")
                if ids_cache_name:
                    ids_cache[ids_cache_name] = data_df[primary_key].tolist()

                request_children = t.get('children', [])

                for i, child in enumerate(request_children):
                    child_list = []
                    if child['ref'] in data_df.columns:
                        child_list = data_df[child['ref']].tolist()

                    if child_list:
                        cpath = child['path']
                        childParam = child['param']

                        args = child.get('args')
                        if args:
                            args[childParam] = child_list
                            jsonArgs = json.dumps(args)
                            args = jsonArgs
                        else:
                            args = '{"advertiser_id": "%s", "%s":%s}' % (ADVERTISER_ID, childParam, child_list)
                        childDataResponse = get_data(cpath, args)

                        mapname = child['mapname']
                        if bool(childDataResponse['data']) and "mapname" in child:
                            primary_key_child = child['child_id']
                            if mapname in childDataResponse["data"]:
                                crow = []
                                try:
                                    for child in child_list:
                                        crow.append(childDataResponse["data"][mapname][str(child)])

                                    df_child = db_action(
                                        input_object=crow,
                                        schema_name="tiktok",
                                        table_name=mapname,
                                        primary_key_name=primary_key_child
                                    )

                                    print(Fore.GREEN + title + "_child " + mapname + Style.RESET_ALL,
                                          len(df_child.index),
                                          " rows loaded")
                                except Exception as e:
                                    exc_type, exc_obj, exc_tb = sys.exc_info()
                                    message = f'An error occurred for child {i}: {e}'
                                    print(Fore.RED + message + Style.RESET_ALL, "at line:", exc_tb.tb_lineno, "\n", d)
                                    continue
                            else:
                                print(Fore.YELLOW + str(path) + Style.RESET_ALL, childDataResponse["data"],
                                      " mapname :", mapname)
                        else:
                            print(Fore.YELLOW + title + "_child " + mapname + Style.RESET_ALL, " JSON empty")
                print(Fore.GREEN + title + Style.RESET_ALL, len(data_df.index), " rows loaded")
            else:
                print(Fore.YELLOW + title + Style.RESET_ALL + " JSON is Empty")


    def main(tasks, **kwargs):
        recreate_access_token = False
        if recreate_access_token:
            generate_access_token()

        execute_tasks(tasks)


    tiktok_task = PythonOperator(
        task_id='tiktok_request',
        python_callable=main,
        op_kwargs={
            'tasks': tasks
        }
    )

    tiktok_async_report = PythonOperator(
        task_id='tiktok_async_report',
        python_callable=create_report,
    )

    tiktok_async_blockedword = PythonOperator(
        task_id='tiktok_async_blockedword',
        python_callable=blockedword_task,
    )

    tiktok_task >> tiktok_async_blockedword >> tiktok_async_report
