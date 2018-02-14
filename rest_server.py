from flask import Flask, request, Response
from flask_restful import Resource, Api
from json import dumps
from flask_jsonpify import jsonify
import MySQLdb
from functools import wraps

app = Flask(__name__)
api = Api(app)

def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return username == 'admin' and password == 'secret'

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

class Status(Resource):
    def get(self):
        return {'status': 'ok'}

class SensorData(Resource):
    @requires_auth
    def get(self):
        db = MySQLdb.connect(host="localhost", user="klimasensor", passwd="klimasensor", db="klimasensordb")
        cur = db.cursor()
        try:
            cur.execute("SELECT * FROM data")
            rows = cur.fetchall()
            result = [SensorRow(row).serialize() for row in rows]
            print("Sending " + str(len(result)) + " records")
            db.close()
            return jsonify(result)

        except Exception as e:
            print(e)
            db.close()
            return

class SensorDataFrom(Resource):
    @requires_auth
    def get(self, fromTs):
        db = MySQLdb.connect(host="localhost", user="klimasensor", passwd="klimasensor", db="klimasensordb")
        cur = db.cursor()
        try:
            fromTs = str(fromTs).replace("%20", "T")
            cur.execute("SELECT * FROM data WHERE Timestamp > %s", (fromTs,))
            rows = cur.fetchall()
            result = [SensorRow(row).serialize() for row in rows]
            print("Sending " + str(len(result)) + " records")
            db.close()
            return jsonify(result)

        except Exception as e:
            print(e)
            db.close()
            return

class SensorRow:
    timestamp = None
    pressure = None
    temperature = None
    humidity = None

    def __init__(self, array):
        if (len(array) >= 4):
            self.timestamp = array[0]
            self.humidity = array[1]
            self.temperature = array[2]
            self.pressure = array[3]

    def serialize(self):
        return {
            'timestamp': self.timestamp,
            'humidity': self.humidity,
            'temperature': self.temperature,
            'pressure': self.pressure,
        }


api.add_resource(Status, '/status')
api.add_resource(SensorData, '/sensordata')
api.add_resource(SensorDataFrom, '/sensordata/<fromTs>')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
    app.config.update(
        DATABASE_URI='sqlite:///flask-openid.db',
        SECRET_KEY='development key',
        DEBUG=True
    )