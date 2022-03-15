import os

import yaml

from other import PROJECT_ROOT

source_filename = "env.yaml"
dest_filename = ".env"

env_name = "airflow-2"


def read_file(source_file):
    with open(os.path.join(PROJECT_ROOT, source_file), 'r') as source:
        try:
            envs_list = yaml.safe_load(source)
        except yaml.YAMLError as exc:
            print(exc)
            return None
        return envs_list['secrets'][0]['secrets']


def write_dotenv(env_vars_list, dest_filename):
    with open(os.path.join(PROJECT_ROOT, dest_filename), 'w') as dest:
        for secret in env_vars_list:
            value = secret['value']
            if ' ' in value:
                dest.write(f'{secret["name"]}="{secret["value"]}"\n')
            else:
                dest.write(f"{secret['name']}={value}\n")


if __name__ == '__main__':
    env_name = os.environ.get("ENV_NAME", env_name)

    env_secrets = read_file(source_filename)
    print(env_secrets)
    write_dotenv(env_secrets, dest_filename)
