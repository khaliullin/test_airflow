import os
from datetime import datetime
import pandas_redshift as pr


def main(base_database, base_schema, base_table, target_database, target_schema, target_table, grant_user=None, **kwargs):

    pr.connect_to_redshift(
        dbname=base_database,
        host=os.environ['DATABASE_HOST'],
        port=5439,
        user=os.environ['DATABASE_USERNAME'],
        password=os.environ['DATABASE_PASSWORD']
    )

    columns = pr.redshift_to_pandas(
        """SELECT column_name, data_type FROM information_schema.columns WHERE TABLE_NAME = '{}' ORDER BY ordinal_position """.format(base_table))
    columns = dict(zip(columns['column_name'], columns['data_type']))

    columns_joined = ''

    for key, value in columns.items():

        if columns_joined == '':
            pass

        else:
            columns_joined = columns_joined + ' , '

        columns_joined = columns_joined + '"' + key + '"' + ' ' + value

    csv_name = base_schema + base_table + str(datetime.now())

    pr.exec_commit("""
    UNLOAD ('select * from {}.{}')
    TO 's3://away-data/s3_to_redshift/{}'
    access_key_id '{}'
    secret_access_key '{}'
    HEADER
    CSV
    """.format(base_schema, base_table, csv_name, os.environ['AWS_DATAS3_ACCESS_KEY'], os.environ['AWS_DATAS3_ACCESS_SECRET']))

    file = 's3://away-data/s3_to_redshift/{}'.format(csv_name)

    pr.connect_to_redshift(
        dbname=target_database,
        host=os.environ['DATABASE_HOST'],
        port=5439,
        user=os.environ['DATABASE_USERNAME'],
        password=os.environ['DATABASE_PASSWORD']
    )

    target_columns = pr.redshift_to_pandas(
        """SELECT column_name, data_type FROM information_schema.columns WHERE TABLE_NAME = '{}' ORDER BY ordinal_position """.format(target_table))
    target_columns = dict(
        zip(target_columns['column_name'], target_columns['data_type']))

    pr.exec_commit(""" DROP TABLE IF EXISTS {}.{} CASCADE """.format(
        target_schema, target_table))
    pr.exec_commit("""CREATE TABLE {}.{} {} """.format(
        target_schema, target_table, '(' + columns_joined + ')'))

    pr.exec_commit("""
    COPY {}."{}"
    FROM '{}'
    access_key_id '{}'
    secret_access_key '{}'
    IGNOREHEADER 1
    TRUNCATECOLUMNS
    CSV
    """.format(target_schema, target_table, file, os.environ['AWS_DATAS3_ACCESS_KEY'], os.environ['AWS_DATAS3_ACCESS_SECRET']))

    if grant_user is not None:
        pr.exec_commit(""" GRANT ALL ON TABLE {}.{} TO {} """.format(
            target_schema, target_table, grant_user))
