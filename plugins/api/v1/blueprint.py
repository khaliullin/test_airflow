#
# from flask import Blueprint
# from plugins.api.version import version
# from flask_restx import Resource, Api
# from airflow.www.app import csrf
# from flask_restx.reqparse import RequestParser
#
# # curl http://127.0.0.1:8080/api/v1/health
# # https://github.com/uptake/airflow-api/tree/1bde132ec5f59da56c94cb0fd851c8854fcdc9bf
# #
#
# URL_PREFIX = "/api/v1"
# BLUEPRINT_NAME = "v1"
# DOCUMENTATION_ROUTE = "/doc"
# HEALTH_ROUTE = "/health"
# TICKTOK_CALLBACK = "/tiktokCallback"
# VARIABLES_RESOURCE_ROUTE = '/variables'
# DAGS_RESOURCE_ROUTE = "/dags"
# DAG_RUNS_RESOURCE_ROUTE = "/dag-runs"
# DAG_FILES_RESOURCE_ROUTE = "/files"
#
# blueprint = Blueprint(BLUEPRINT_NAME, __name__, url_prefix=URL_PREFIX)
# csrf.exempt(blueprint)
#
#
# @blueprint.record
# def record_params(setup_state):
#     app = setup_state.app
#     app.config["ERROR_404_HELP"] = False
#
#
# api = Api(
#     blueprint,
#     title='Airflow API',
#     version=version,
#     description='API for Pulling Data From Airflow',
#     doc=DOCUMENTATION_ROUTE
# )
#
# from plugins.api.v1.health import Health
# api.add_resource(Health, HEALTH_ROUTE)
#
# from plugins.api.v1.tiktok_callback import TiktokCallback
# api.add_resource(TiktokCallback, TICKTOK_CALLBACK)
