import os
import logging
import pandas as pd
import numpy as np
import simplejson as json
from datetime import datetime, date, timedelta
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow import settings
from sqlalchemy import create_engine
import boto3
import zipfile
import io
import pytz
from jinja2 import Environment, BaseLoader
from plugins.redshift import *


class S3ToRedshiftOperator(BaseOperator):

    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#FFF5EE'

    #@apply_defaults
    def __init__(
            self, schema, table, bucket,
            prefix, columns, unique_key, copyParams, pre_load_hook=None,
            s3_key='AWS_DATAS3_ACCESS_KEY', s3_secret='AWS_DATAS3_ACCESS_SECRET', extension=None,
            bypass_past_loads=False,
            ** kwargs):

        super(S3ToRedshiftOperator, self).__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.bucket = bucket
        self.prefix = prefix
        self.s3_key = s3_key
        self.s3_secret = s3_secret
        self.columns = columns
        self.unique_key = unique_key
        self.copyParams = copyParams
        self.pre_load_hook = pre_load_hook
        self.s3path = None
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.environ[self.s3_key],
            aws_secret_access_key=os.environ[self.s3_secret]
        )
        self.extension = extension
        self.bypass_past_loads = bypass_past_loads

    def unzip(self, key, **context):

        obj = self.s3.get_object(Bucket=self.bucket, Key=key)

        putObjects = []
        fileList = []

        with io.BytesIO(obj["Body"].read()) as tf:
            tf.seek(0)

            with zipfile.ZipFile(tf, mode='r') as zipf:
                for file in zipf.infolist():
                    fileName = file.filename
                    fileList.append(fileName)
                    putFile = self.s3.put_object(
                        Bucket=self.bucket, Key=fileName, Body=zipf.read(file))
                    putObjects.append(putFile)

        if len(putObjects) > 0:
            deletedObj = self.s3.delete_object(Bucket=self.bucket, Key=key)

        return fileList

    def execute(self, context):

        meta_db = create_engine(os.environ['DATABASE_URL'])

        if self.bypass_past_loads == False:

            try:
                past_loads = [r.path for r in meta_db.execute(
                    'SELECT DISTINCT path FROM s3_load_history').fetchall()]
            except:
                past_loads = []

        else:

            past_loads = []

        objects = []
        listObjectArgs = {'Bucket': self.bucket, 'Prefix': self.prefix}

        while True:

            getObjects = self.s3.list_objects_v2(**listObjectArgs)

            if getObjects['KeyCount'] == 0:
                break

            else:

                for object in getObjects["Contents"]:

                    if object["Key"] not in past_loads:

                        if object["Key"].lower()[-4:] == '.zip':
                            unzip = self.unzip(key=object["Key"])
                            for item in unzip:
                                objects.append(item)

                        else:
                            objects.append(object["Key"])

                try:
                    listObjectArgs['ContinuationToken'] = getObjects['NextContinuationToken']
                except:
                    break

        logging.info('Loading files {}'.format(objects))

        columns_joined = ''
        for key, value in self.columns.items():
            columns_joined = columns_joined + '"' + key + '"' + ' ' + value + ','

        for object in objects:

            if self.extension is None or object.endswith(self.extension):

                self.s3path = 's3://' + self.bucket + '/' + object

                if self.pre_load_hook is not None:
                    pre_load_hook_template = Environment(
                        loader=BaseLoader()).from_string(self.pre_load_hook)
                    exec(pre_load_hook_template.render())

                RedshiftOperation("""DROP TABLE IF EXISTS {}."{}_stg" """.format(
                    self.schema, self.table))
                RedshiftOperation("""CREATE TABLE {}."{}_stg" {} """.format(
                    self.schema, self.table, '(' + columns_joined[:-1] + ')'))

                RedshiftOperation("""
                    COPY {}."{}_stg"
                    FROM '{}'
                    access_key_id '{}'
                    secret_access_key '{}'
                    {}
                    """.format(self.schema, self.table, self.s3path, os.environ[self.s3_key], os.environ[self.s3_secret], self.copyParams)
                )

                RedshiftOperation("""ALTER TABLE {}."{}_stg" ADD COLUMN "source_file" varchar DEFAULT '{}' """.format(
                    self.schema, self.table, object))

                if isinstance(self.unique_key, (list,)):

                    RedshiftOperation("""ALTER TABLE {}."{}_stg" ADD COLUMN "id" varchar(256) """.format(
                        self.schema, self.table, object))

                    stmt = "concat('id',"
                    for x in self.unique_key[:-1]:
                        stmt = stmt + 'concat(' + '"' + x + '",'
                    stmt = stmt + '"' + self.unique_key[-1] + '"'
                    stmt = stmt + ')' * len(self.unique_key)

                    RedshiftOperation("""UPDATE {}.{}_stg
                                        SET id = MD5({}) """.format(self.schema, self.table, stmt))

                    unique_key_col = 'id'

                else:
                    unique_key_col = self.unique_key

                RedshiftOperation("""DELETE FROM {}.{}
                                     WHERE {} in (SELECT {} FROM {}.{}_stg) """.format(self.schema, self.table, unique_key_col, unique_key_col, self.schema, self.table))

                RedshiftOperation("""INSERT INTO {}."{}" SELECT DISTINCT * FROM {}."{}_stg" """.format(
                    self.schema, self.table, self.schema, self.table))

                RedshiftOperation("""DROP TABLE {}."{}_stg" """.format(
                    self.schema, self.table))

                results = pd.DataFrame(
                    [[context['dag'].dag_id, context['task'].task_id, context['ts'], object, datetime.utcnow()]])
                results.columns = ['dag_id', 'task_id',
                                   'execution_date', 'path', 'load_ts']
                results.to_sql('s3_load_history', meta_db,
                               if_exists='append', index=False)
