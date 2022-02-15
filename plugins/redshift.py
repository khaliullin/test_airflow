import os
import pandas as pd
import numpy as np
import hashlib
from io import StringIO
import traceback
import psycopg2
import boto3
import sys
import re
import uuid


def init():
    global connect, cursor, s3
    connect = psycopg2.connect(dbname=os.environ['DATABASE_NAME'],
                               host=os.environ['DATABASE_HOST'],
                               port=5439,
                               user=os.environ['DATABASE_USERNAME'],
                               password=os.environ['DATABASE_PASSWORD']
                               )

    cursor = connect.cursor()

    s3 = boto3.resource('s3',
                        aws_access_key_id=os.environ['AWS_DATAS3_ACCESS_KEY'],
                        aws_secret_access_key=os.environ['AWS_DATAS3_ACCESS_SECRET']
                        )


def RedshiftQuery(query):
    # pass a sql query and return a pandas dataframe
    init()
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=[
        desc[0] for desc in cursor.description])
    return df


def RedshiftOperation(query):
    init()
    cursor.execute(query)
    connect.commit()


def validate_column_names(df):
    # check for spaces in the column names
    # and delimit them if there are
    there_are_spaces = sum(
        [re.search('\s', x) is not None for x in df.columns]) > 0
    if there_are_spaces:
        col_names_dict = {x: '"{0}"'.format(x) for x in df.columns}
        df.rename(columns=col_names_dict, inplace=True)
    return df


def pd_dtype_to_redshift_dtype(dtype):
    if dtype.startswith('int'):
        return 'INTEGER'
    elif dtype.startswith('float'):
        return 'REAL'
    elif dtype.startswith('datetime'):
        return 'TIMESTAMP'
    elif dtype == 'bool':
        return 'BOOLEAN'
    else:
        return 'VARCHAR(256)'


def df_to_s3(df, csv_name, delimiter):
    init()
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep=delimiter)
    s3.Bucket('away-data').put_object(
        Key='s3_to_redshift/' + csv_name, Body=csv_buffer.getvalue())


def create_redshift_table(df,
                          table_name,
                          dtypes=None,
                          diststyle='even',
                          distkey='',
                          sort_interleaved=False,
                          sortkey=''):
    init()

    columns = list(df.columns)
    if dtypes is None:
        dtypes = [pd_dtype_to_redshift_dtype(dtype.name)
                  for dtype in df.dtypes.values]
    columns_and_data_type = ', '.join(
        ['{0} {1}'.format(x, y) for x, y in zip(columns, dtypes)])

    create_table_query = 'create table {0} ({1})'.format(
        table_name, columns_and_data_type)
    if not distkey:
        # Without a distkey, we can set a diststyle
        create_table_query += ' diststyle {0}'.format(diststyle)
    else:
        # otherwise, override diststyle with distkey
        create_table_query += ' distkey({0})'.format(distkey)
    if len(sortkey) > 0:
        if sort_interleaved:
            create_table_query += ' interleaved'
        create_table_query += ' sortkey({0})'.format(sortkey)
    RedshiftOperation('drop table if exists {0}'.format(table_name))
    RedshiftOperation(create_table_query)


def s3_to_redshift(table_name, csv_name, delimiter=',', quotechar='"',
                   dateformat='auto', timeformat='auto', parameters=''):

    init()

    s3_to_sql = """
    copy {0}
    from 's3://away-data/s3_to_redshift/{1}'
    delimiter '{2}'
    ignoreheader 1
    csv quote as '{3}'
    dateformat '{4}'
    timeformat '{5}'
    access_key_id '{6}'
    secret_access_key '{7}'
    {8};
    """.format(table_name, csv_name, delimiter, quotechar, dateformat,
               timeformat, os.environ['AWS_DATAS3_ACCESS_KEY'], os.environ['AWS_DATAS3_ACCESS_SECRET'], parameters)

    try:
        RedshiftOperation(s3_to_sql)
    except Exception as e:
        print(e)
        traceback.print_exc(file=sys.stdout)
        connect.rollback()
        raise


def pandas_to_redshift(df,
                       table_name,
                       dtypes=None,
                       delimiter=',',
                       quotechar='"',
                       dateformat='auto',
                       timeformat='auto',
                       append=False,
                       diststyle='even',
                       distkey='',
                       sort_interleaved=False,
                       sortkey='',
                       parameters='',
                       **kwargs):

    # Validate column names.
    df = validate_column_names(df)
    # Send data to S3
    csv_name = '{}-{}.csv'.format(table_name, uuid.uuid4())
    df_to_s3(df, csv_name, delimiter)

    # create an empty table in redshift
    if not append:
        create_redshift_table(df, table_name, dtypes,
                              diststyle, distkey, sort_interleaved, sortkey)

    # create the copy statement to send from s3 to redshift
    s3_to_redshift(table_name, csv_name, delimiter, quotechar,
                   dateformat, timeformat, parameters)


def RedshiftMaxValue(schema, table, column):

    try:
        maxLoaded = RedshiftQuery('SELECT MAX({}) FROM {}.{}'.format(
            column, schema, table))['max'].values[0]
    except:
        maxLoaded = 0

    return maxLoaded


def RedshiftLoad(data, schema, table, unique_key):

    table = table.lower()

    if not data.empty:

        data = data.dropna(axis=0, how='all')
        data.columns = map(str.lower, data.columns.astype(str))
        df_final = pd.DataFrame()

# Generate unique key

        if isinstance(unique_key, (list,)):

            data['id'] = 'id'
            for column in unique_key:

                data['id'] = data['id'] + data[column.lower()].astype(str)

            data['id'] = data['id'].apply(lambda d: d.encode('utf-8'))
            data['id'] = [hashlib.md5(val).hexdigest() for val in data['id']]

            unique_key = 'id'

# Load data to redshift and deduplicate

        table_exists = RedshiftQuery("""SELECT count(distinct table_name)
                                        FROM information_schema.columns
                                        WHERE table_schema = '{}' and table_name = '{}'""".format(schema, table)
                                     )['count'].values[0]

        if table_exists == 1:
            existing_columns = RedshiftQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = {} and table_name = {} ORDER BY ordinal_position".format(
                "'" + schema + "'", "'" + table + "'")).to_dict(orient='list')
            existing_columns = dict(
                zip(existing_columns['column_name'], existing_columns['data_type']))

            for column, dtype in existing_columns.items():

                if column not in data.columns:
                    data[column] = ''
                if dtype == 'bigint' or dtype == 'integer':
                    df_final[column] = data[column].replace(
                        'None', np.nan).replace('', np.nan).fillna(0).astype(int)
                elif dtype == 'double precision' or dtype == 'numeric':
                    df_final[column] = data[column].replace(
                        'None', np.nan).replace('', np.nan).fillna(0).astype(float)
                elif dtype == 'timestamp without time zone' or dtype == 'timestamp with time zone' or dtype == 'date':
                    df_final[column] = pd.to_datetime(data[column])
                elif 'boolean' in dtype:
                    df_final[column] = data[column].astype('bool')
                else:
                    df_final[column] = data[column].astype(object)

            data = df_final
            RedshiftOperation("""DELETE FROM {}.{} WHERE "{}" in ({})""".format(schema, table, unique_key, ",".join(
                "'" + str(x).replace("'", "''") + "'" for x in data[unique_key.lower()].to_list())))

        else:
            data_types = {}
            for key, value in data.dtypes.to_dict().items():

                if str(value) == 'datetime64[ns]':
                    data_types[key] = 'timestamp'
                elif str(value) == 'int64':
                    data_types[key] = 'int'
                elif str(value) == 'float64':
                    data_types[key] = 'float'
                elif str(value) == 'bool':
                    data_types[key] = 'bool'
                else:
                    try:
                        data[key] = data[key].apply(
                            lambda d: pd.to_datetime(str(d)))
                        data_types[key] = 'timestamp'
                    except:
                        data_types[key] = 'varchar({})'.format(
                            str(data[key].astype(str).map(len).max() * 5))

                columns_joined = ''
                for key, value in data_types.items():
                    columns_joined = columns_joined + '"' + key + '"' + ' ' + value

                    if key == unique_key:
                        columns_joined = columns_joined + ' UNIQUE'

                    columns_joined = columns_joined + ','

            RedshiftOperation("""CREATE TABLE {}."{}" {} """.format(
                schema, table, '(' + columns_joined[:-1] + ')'))

        ## aat comment this ?
        print("Loading to redshift...\n{}\n{}".format(data, data.dtypes))
        ##  
        pandas_to_redshift(df=data, table_name='{}.{}'.format(
            schema, table), append=True, parameters='MAXERROR 10')
