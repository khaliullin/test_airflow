import os
import yaml

from other import PROJECT_ROOT

"""
Read .env from project root and create env.yaml for future encryption.
"""

source_filename = ".env.local"
dest_file = "env.yaml"

env_name = "airflow-2"


def read_file(source_file):
    env_vars_dict = {}
    key_separator = "="

    with open(os.path.join(PROJECT_ROOT, source_file), 'r') as source:
        for line in source.readlines():
            if key_separator not in line:
                key_separator = ":"
            key = line.split(key_separator)[0].strip().replace("'", "").replace('"', '')
            value = line.split(key_separator)[1].strip().replace("'", "").replace('"', '')
            if value.endswith(","):
                value = value[:-1]
            env_vars_dict[key] = value
            print(key, end=": ")
            print(value)

        return env_vars_dict


def write_yaml(env_vars_dict, dest_filename):
    d = {
        'secrets': [
            {
                'environment': env_name,
                'secrets': [
                    {'name': key, 'value': value, 'description': key} for key, value in env_vars_dict.items()
                ]
            }
        ]
    }

    with open(os.path.join(PROJECT_ROOT, dest_filename), 'w') as dest:
        yaml.dump(d, dest, sort_keys=False)


if __name__ == '__main__':
    env_name = os.environ.get("ENV_NAME", env_name)

    keys_dict = read_file(source_filename)
    write_yaml(keys_dict, dest_file)
