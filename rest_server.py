from flask import Flask, request
from flask_restful import Resource, Api
from json import dumps
from flask.ext.jsonpify import jsonify
import MySQLdb

db = MySQLdb.connect(host="localhost",user="klimasensor",passwd="klimasensor",db="klimasensordb")
app = Flask(__name__)
api = Api(app)


class SensorData(Resource):
    def get(self, fromTs):
        cur = self.db.cursor()
        try:
            cur.execute("SELECT * FROM data WHERE Timestamp > %s", (str(fromTs),))
            rows = cur.fetchall()
            result = {'sensorData': [row for row in rows]}
            return jsonify(result)

        except:
            return

api.add_resource(SensorData, '/sensordata/<fromTs>')  # Route_1

if __name__ == '__main__':
    app.run(port='5000')