import logging
import requests
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import simplejson as json
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from plugins.redshift import *
from jinja2 import Environment, BaseLoader


class ApiToRedshiftOperator(BaseOperator):

    template_fields = ('parameters', 'headers',)
    template_ext = tuple()
    ui_color = '#7BA05B'

    #@apply_defaults
    def __init__(
            self, base, schema, table, unique_key,
            endpoint=None, parameters='', headers={},
            data={}, resultPath=[], nextPath=None,
            nextLink='', orient=None, incremental_key=None,
            post_load_hook=None, pre_load_hook=None, contextCols={},
            auth=None, method='GET', reset_index=False, **kwargs):

        super(ApiToRedshiftOperator, self).__init__(**kwargs)
        self.base = base
        self.endpoint = endpoint
        self.schema = schema
        self.table = table
        self.unique_key = unique_key
        self.parameters = parameters
        self.headers = headers
        self.data = data
        self.resultPath = ['response'] + resultPath
        self.nextPath = nextPath
        self.nextLink = nextLink
        self.orient = orient
        self.incremental_key = incremental_key
        self.pre_load_hook = pre_load_hook
        self.post_load_hook = post_load_hook
        self.contextCols = contextCols
        self.df = None
        self.auth = auth
        self.method = method
        self.reset_index = reset_index

    def getDataFromPath(self, data, path, **context):

        for x in path:
            if x in data or isinstance(x, int):
                data = data[x]
            else:
                data = None
                break
        return json.dumps(data)

    def apiCall(self, url=None, **context):

        if url is None:
            url = '{}/{}{}'.format(self.base, self.endpoint, self.parameters)

        logging.info(
            "Making call to {}".format(url)
        )

        response = requests.request(
            method=self.method,
            url=url.replace('"', ''),
            headers=self.headers,
            data=self.data,
            auth=self.auth
        )

        if response.status_code != 200: 
             ##
             ## 09/24/2021
             ## @andreitoropov added for the API troubleshooting  
             ## url
             ## headers
             ## 
            raise Exception(str(response.status_code) + ' ' + response.text  +  
                '\n method  : ' + str(self.method)  + 
                '\n url     : ' + url  + 
                '\n headers : ' + str(self.headers) + 
                '\n auth    : ' + str(self.auth)
                )

        else:
            responseItems = response.json()
            for path in self.resultPath[1:]:
                responseItems = responseItems[path]

        if 'lastResponse' not in context:
            iter = 1
            nextIter = 2
            totalResponseItems = len(responseItems)
        else:
            iter = context['lastResponse']['meta']['iter'] + 1
            nextIter = iter + 1
            totalResponseItems = context['lastResponse']['meta']['totalResponseItems'] + len(
                responseItems)

        response = {
            'response': response.json(),
            'meta': {
                'links': dict(response.links),
                'headers': dict(response.headers),
                'iter': iter,
                'nextIter': nextIter,
                'totalResponseItems': totalResponseItems
            }
        }

        logging.info(
            "Success! Response is...\n" + json.dumps(response, indent=4)
        )

        return response

    def dictFlattener(self, **context):

        df = self.df

        dict_cols = None
        while dict_cols != []:

            dict_cols = (df.applymap(type) == dict).any()
            dict_cols = dict_cols.index[dict_cols].tolist()

            for column in list(df.columns.values):

                if column in dict_cols:

                    dfList = df[column].tolist()
                    dfList = [{} if not isinstance(
                        item, dict) else item for item in dfList]
                    dictdf = pd.DataFrame(dfList).add_prefix(
                        '{}_'.format(column))
                    df = df.drop(columns=[column])
                    df = df.merge(dictdf, how='left',
                                  left_index=True, right_index=True)

        return df

    def listLoader(self, **context):

        df = self.df

        if not isinstance(self.unique_key, (list,)):

            list_cols = (df.applymap(type) == list).any()
            list_cols = list_cols.index[list_cols].tolist()

            for item in list(df.columns.values):

                if item in list_cols:

                    try:

                        stg_sub_df = df.dropna(subset=[item])
                        sub_df = pd.DataFrame({
                            col: np.repeat(
                                stg_sub_df[col].values, stg_sub_df[item].str.len())
                            for col in stg_sub_df.columns.drop(item)}
                        ).assign(**{item: np.concatenate(stg_sub_df[item].values)})[stg_sub_df.columns]

                        sub_df = sub_df[[self.unique_key, item]]
                        sub_df['{}_id'.format(self.table)
                               ] = sub_df[self.unique_key]
                        sub_df = sub_df.drop([self.unique_key], axis=1)

                        if (sub_df.applymap(type) == dict).any()[item] == True:
                            dict_col_df = json_normalize(
                                sub_df[item].dropna(), sep='_')
                            sub_df = sub_df.merge(
                                dict_col_df, how='left', left_index=True, right_index=True)
                            sub_df = sub_df.drop([item], axis=1)

                        df = df.drop(columns=[item])
                        RedshiftLoad(data=sub_df, schema=self.schema, table='{}__{}'.format(
                            self.table, item), unique_key='{}_id'.format(self.table))

                    except:
                        pass
        return df

    def execute(self, context):

        response = self.apiCall(
            base=self.base,
            endpoint=self.endpoint,
            parameters=self.parameters,
            headers=self.headers,
            data=self.data,
            auth=self.auth
        )

        dataToLoad = self.getDataFromPath(
            data=response,
            path=self.resultPath
        )

        if dataToLoad != [] and dataToLoad != '[]':

            self.df = pd.read_json(
                path_or_buf=dataToLoad,
                orient=self.orient,
                convert_dates=False,
                dtype=False

            )

            if self.nextPath is not None:

                while True:

                    if self.incremental_key is not None:

                        currentMax = RedshiftMaxValue(
                            schema=self.schema,
                            table=self.table,
                            column=self.incremental_key
                        )

                        if str(self.dictFlattener(df=self.df)[self.incremental_key].min()) <= str(currentMax):
                            break

                    next = self.getDataFromPath(
                        data=response['response'] if self.nextPath[0] != 'meta' else response,
                        path=self.nextPath
                    )

                    if next == None or next.lower() == 'null':
                        break

                    response = self.apiCall(
                        url=self.nextLink + next,
                        headers=self.headers,
                        data=self.data,
                        auth=self.auth,
                        lastResponse=response
                    )

                    dataToLoad = self.getDataFromPath(
                        data=response,
                        path=self.resultPath
                    )

                    if dataToLoad == [] or dataToLoad == '[]':
                        break

                    dataToLoad = pd.read_json(
                        path_or_buf=dataToLoad, orient=self.orient, convert_dates=False, dtype=False)

                    if self.df.equals(dataToLoad) is True:
                        break

                    self.df = self.df.append(
                        other=dataToLoad,
                        ignore_index=True
                    )

            if self.reset_index is True:

                self.df.drop(labels='index', axis='columns',
                             errors='ignore', inplace=True)
                self.df.index.name = 'index'
                self.df.reset_index(inplace=True)

            self.df = self.dictFlattener()
            self.df = self.listLoader()

            for col, value in self.contextCols.items():
                self.df[col] = value

            if self.pre_load_hook is not None:
                pre_hook_template = Environment(
                    loader=BaseLoader()).from_string(self.pre_load_hook)
                exec(pre_hook_template.render())

            RedshiftLoad(
                data=self.df,
                schema=self.schema,
                table=self.table,
                unique_key=self.unique_key
            )

            if self.post_load_hook is not None:
                post_hook_template = Environment(
                    loader=BaseLoader()).from_string(self.post_load_hook)
                exec(post_hook_template.render())
