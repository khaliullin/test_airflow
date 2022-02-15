# import sys
# from flask_restx import Resource, fields, reqparse
# from flask_restx.reqparse import RequestParser
# import requests
# # from plugins.api.v1.blueprint import api
# from plugins.api.version import version
# import os
# import json
# from urllib.parse import urlencode, urlunparse  # noqa
#
#
# app_id = os.environ['TIKTOK_APPID']
# secret = os.environ['TIKTOK_SECRET']
#
# parser = reqparse.RequestParser(bundle_errors=True)
# parser.add_argument("auth_code", type=str)
# parser.add_argument("code", type=str)
# parser.add_argument("state", type=str)
# parser.add_argument("token", type=str)
#
# tiktokCallback = api.model('TiktokCallback', {
#     'version': fields.String,
#     'success': fields.Boolean
# })
#
# def build_url(path, query=""):
#     # type: (str, str) -> str
#     """
#     Build request URL
#     :param path: Request path
#     :param query: Querystring
#     :return: Request URL
#     """
#     scheme, netloc = "https", "business-api.tiktok.com"
#     return urlunparse((scheme, netloc, path, "", query, ""))
#
#
# def accessToken(json_str):
#     # type: (str) -> dict
#     PATH = "/open_api/v1.2/oauth2/access_token/"
#     """
#     Send POST request
#     :param json_str: Args in JSON format
#     :return: Response in JSON format
#     """
#     url = build_url(PATH)
#     args = json.loads(json_str)
#     headers = {
#         "Content-Type": "application/json"
#     }
#     response = requests.post(url, headers=headers, json=args)
#     return response.json()
#
# def getAccessToken(auth_code):
#     accessArgs = "{\"secret\": \"%s\", \"app_id\": \"%s\", \"auth_code\": \"%s\"}" % (secret, app_id, auth_code)
#     response = accessToken(accessArgs)
#     print(response)
#     message = response["message"]
#     if "data" in response and "access_token" in response["data"]:
#         return response["data"]["access_token"]
#     else:
#         print('Warning message: %s' % message)
#         print(response)
#         return ""
#
# class TiktokCallback(Resource):
#
#     @api.expect(parser)
#     @api.response(200, "Success", tiktokCallback)
#     def get(self):
#         success = True
#         message = "Ok"
#         try:
#             # args = parser.parse_args()
#             # print(":: TiktokCallback ::", args)
#             auth_code = parser.parse_args()["auth_code"]
#             print(":: auth_code      ::", auth_code)
#             # token = parser.parse_args()["state"]
#             accessToken = getAccessToken(auth_code)
#             # s = doDecrypt(token)
#             if accessToken:
#                 message = accessToken
#                 os.environ["TIKTOK_ACCESS_TOKEN"] = accessToken
#             else:
#                  success = False
#                  message = "Access denied"
#         except Exception as e:
#             exc_tb = sys.exc_info()
#             success = False
#             message = "Access denied"
#             os.environ["TIKTOK_ACCESS_TOKEN"] = ""
#             print('An error occurred: %s' % e, "at line:", exc_tb.tb_lineno)
#
#         """Retrieve version and health of API"""
#         response = {
#             "version": version,
#             "success": success,
#             "message": message
#         }
#         return response
