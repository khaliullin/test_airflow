import os
import requests

from other import PROJECT_ROOT

destination_filename = '.env.local'


def write_file(filename):
    headers = {
        'Authorization': 'Bearer {}'.format(os.environ['HEROKU_KEY']),
        'Accept': 'application/vnd.heroku+json; version=3'
    }

    env_vars = requests.get(
        url='https://api.heroku.com/apps/away-airflow-2-production/config-vars',
        headers=headers
    ).json()

    with open(os.path.join(PROJECT_ROOT, filename), 'w') as dest:
        for key, value in env_vars.items():
            dest.write(f"{key}={value}\n")


if __name__ == '__main__':
    write_file(destination_filename)
