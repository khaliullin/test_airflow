from flask_restx import Resource, fields
from blueprint import api
from api.version import version

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
