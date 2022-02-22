from flask_restx import Resource, fields
from plugins.api.v1.blueprint import api
from plugins.api.version import version

health = api.model('Health', {
    'version': fields.String,
    'health': fields.String
})


class Health(Resource):
    @api.response(200, "Success", health)
    def get(self):
        """Retrieve version and health of API"""
        response = {
            "version": version,
            "health": "ok"
        }
        return response
