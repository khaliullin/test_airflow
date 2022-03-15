import os

import requests

from other import PROJECT_ROOT

LOCAL = 'local'
STAGING = 'staging'
PRODUCTION = 'production'

ALLOWED_ENV = (LOCAL, STAGING, PRODUCTION)


class EnvVarsManager:
    def __init__(self, env=LOCAL) -> None:
        self.env = env
        self.env_file = f'.env.{env}'
        self.check_env()

    def check_env(self):
        if self.env not in ALLOWED_ENV:
            raise Exception(f"Environment {self.env} is not supported. Use one of {ALLOWED_ENV}")

    def load_from_heroku(self):
        if self.env == LOCAL:
            return None

        headers = {
            'Authorization': 'Bearer {}'.format(os.environ['HEROKU_KEY']),
            'Accept': 'application/vnd.heroku+json; version=3'
        }
        env_vars = requests.get(
            url=f'https://api.heroku.com/apps/away-airflow-2-{self.env}/config-vars',
            headers=headers
        ).json()

        with open(os.path.join(PROJECT_ROOT, self.env_file), 'w') as dest:
            for key, value in env_vars.items():
                dest.write(f"{key}: {value}\n")

    # def