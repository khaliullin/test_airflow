# @andrew.toropov
# created 03/04/2022

"""
Purpose
Using AWS Secrets Manager to store keys
Shows how to use the AWS SDK for Python (Boto3) with AWS Key Management Service (AWS KMS)
to encrypt and decrypt data.
"""

import base64
import datetime
import json
import logging
import os
import time

import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
 

class DoKeyEncrypt:
    def __init__(self, kms_client):
        self.kms_client = kms_client

    def encrypt(self, key_id, text):
        """
        Encrypts text by using the specified key.

        :param key_id: The ARN or ID of the key to use for encryption.
        :return: The encrypted version of the text.
        """
        # text = input("Enter some text to encrypt: ")  
        try:
            cipher_text = self.kms_client.encrypt(
                KeyId=key_id, Plaintext=text.encode()
            )['CiphertextBlob']
        except ClientError as err:
            logger.error(
                "Couldn't encrypt text. Here's why: %s", err.response['Error']['Message']
            )
        else:
            return cipher_text

    def decrypt(self, key_id, cipher_text):
        """
        Decrypts text previously encrypted with a key.

        @param key_id: The ARN or ID of the key used to decrypt the data.
        @param cipher_text: The encrypted text to decrypt.
        """
        try:
            text = self.kms_client.decrypt(
                KeyId=key_id, CiphertextBlob=cipher_text
            )['Plaintext']
        except ClientError as err:
            logger.error(
                "Couldn't decrypt your ciphertext. Here's why: %s", err.response['Error']['Message']
            )
        else:
            return text.decode()

    def re_encrypt(self, source_key_id, cipher_text):
        """
        Takes ciphertext previously encrypted with one key and reencrypt it by using
        another key.

        @param source_key_id: The ARN or ID of the original key used to encrypt the ciphertext.
        @param cipher_text: The encrypted ciphertext.
        @return: The ciphertext encrypted by the second key.
        """
        destination_key_id = input(
            f"Your ciphertext is currently encrypted with key {source_key_id}. "
            f"Enter another key ID or ARN to reencrypt it: "
        )
        if destination_key_id != '':
            try:
                cipher_text = self.kms_client.re_encrypt(
                    SourceKeyId=source_key_id,
                    DestinationKeyId=destination_key_id,
                    CiphertextBlob=cipher_text
                )['CiphertextBlob']
            except ClientError as err:
                logger.error(
                    "Couldn't reencrypt your ciphertext. Here's why: %s", err.response['Error']['Message']
                )
            else:
                print(f"Reencrypted your ciphertext as: {cipher_text}")
                return cipher_text
        else:
            print("Skipping reencryption demo.")


class DoASM:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/secretsmanager.html#SecretsManager.Client.update_secret
    def __init__(self, secret_name, region_name):
        self.secret_name = secret_name
        self.region_name = region_name

    def set_secret(self,secret):
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html#STS.Client.get_caller_identity
        arn = boto3.client('sts').get_caller_identity()["Arn"]

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/secretsmanager.html#SecretsManager.Client.tag_resource
        ts = time.time()
        ds = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        tags = [
            {
                'Key': 'Caller',
                'Value': f'{arn}'
            },
            {
                'Key': 'Updated',
                'Value': f'{ds}'
            },
        ]

        try:
            response = client.update_secret(SecretId=self.secret_name, SecretString=json.dumps(secret))
            response = client.tag_resource(
                SecretId=self.secret_name,
                Tags=tags
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e

    def get_secret(self):
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=self.secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                return get_secret_value_response['SecretString']
            else:
                return base64.b64decode(get_secret_value_response['SecretBinary'])


if __name__ == '__main__':
    try:
        key_id = "arn:aws:kms:us-east-1:545518596227:key/26c952c5-66d3-4d1d-9187-58671c0df60e"
        headers = {
            'Authorization': 'Bearer {}'.format(os.environ['HEROKU_KEY']),
            'Accept': 'application/vnd.heroku+json; version=3'
        }

        configVars = requests.get(
            url='https://api.heroku.com/apps/away-airflow-production/config-vars',
            headers=headers
        ).json()

        excludeList = [
            'DATABASE_URL', 'REDIS_URL', 'HEROKU_APP_NAME', 'AIRFLOW_VERSION', 'AIRFLOW_HOME',
            'AIRFLOW_EXECUTOR', 'DATABASE_HOST', 'DATABASE_NAME', 'DATABASE_PORT', 'DATABASE_USERNAME',
            'EnvironmentVariables'
        ]

        for item in excludeList:
            configVars.pop(item, None)
            
        configVars['URL'] = 'https://{}.herokuapp.com'.format(os.environ['HEROKU_APP_NAME'])
        key_encrypt = DoKeyEncrypt(boto3.client('kms'))
        ekeys = {}  # encoded keys
        dkeys = {}  # decoded keys
        keys = []
        ebkeys = []   
        for key in configVars:
            value = configVars[key]
            cipher_text = key_encrypt.encrypt(key_id, value)
            if cipher_text is not None: 
                textDecode = key_encrypt.decrypt(key_id, cipher_text)
                ebkey = f'{key}="{textDecode}"'
                # print("[Success]", f"{ebkey}")
                # or encoded
                ekeys[key] = base64.b64encode(cipher_text).decode('ascii')
                # or decoded
                dkeys[key] = textDecode
                if len(' '.join(map(str, ebkeys))) + len(ebkey) < 4096:
                    ebkeys.append(ebkey)
                else:
                    keys.append(ebkeys)
                    ebkeys = []
            else:
                print("[Error]", f'{key}="{value}"')
        
        # $ eb setenv foo=bar JDBC_CONNECTION_STRING=hello PARAM4= PARAM5= 
        if False and len(keys) > 0:
               for ebkeys in keys:
                print(f'{ebkeys}')
                # envlist = '\nexport '.join(map(str, ebkeys))
                # envCmd = f"{envlist}"
                # print(f'{envCmd}')
                # envCmd = f"eb setenv {envlist}"
                # print(f'{envCmd}')
                # print(f'length={len(envCmd)}, count={len(ebkeys)}')
                # ret = os.system(f'{envCmd}')

        # Todo AWS Secrets Manager

        # save decoded keys
        asm = DoASM("airflow-2/staged", "us-east-1")
        if len(dkeys) > 0:
            asm.set_secret(dkeys)

        secret = asm.get_secret()

        # save encoded keys
        asm = DoASM("airflow-2/staged/encoded", "us-east-1")
        if len(ekeys) > 0:
            asm.set_secret(ekeys)
 
        print("=== Done ==")

    except Exception as e:
        logging.exception(f"Exception: {e}")
