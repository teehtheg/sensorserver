from flask import Flask, request, Response
from flask_restful import Resource, Api
from json import dumps
from flask_jsonpify import jsonify
import MySQLdb
import configparser
from functools import wraps

def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return username == config['REST']['username'] and password == config['REST']['password']

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
    def get(self, pageNr):
        if (not pageNr or pageNr == 0):
            pageNr = 0
            cur = db.cursor()
            cur.execute("SELECT * FROM data")
            cache = cur

        try:
            rows = cache.fetchmany(MAX_PAGE)
            result = [SensorRow(row).serialize() for row in rows]
            print("Sending " + str(len(result)) + " records")

            if (not rows or len(rows) < MAX_PAGE):
                return {'data': jsonify(result)}
            else:
                return {'data': jsonify(result), 'next': pageNr+1}

        except Exception as e:
            print(e)
            return

class SensorDataFrom(Resource):
    @requires_auth
    def get(self, fromTs, pageNr):
        if (not pageNr or pageNr == 0):
            pageNr = 0
            cur = db.cursor()
            cur.execute("SELECT * FROM data WHERE Timestamp > %s", (fromTs,))
            cacheFrom = cur

        try:
            rows = cacheFrom.fetchmany(MAX_PAGE)
            result = [SensorRow(row).serialize() for row in rows]
            print("Sending " + str(len(result)) + " records")

            if (not rows or len(rows) < MAX_PAGE):
                return {'data': jsonify(result)}
            else:
                return {'data': jsonify(result), 'next': pageNr+1}

        except Exception as e:
            print(e)
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

app = Flask(__name__)
api = Api(app)

config = configparser.ConfigParser()
config.read('config.ini')

api.add_resource(Status, '/status')
api.add_resource(SensorData, '/sensordata/<pageNr>')
api.add_resource(SensorDataFrom, '/sensordatafrom/<fromTs>/<pageNr>')

MAX_PAGE = 100
db = MySQLdb.connect(host="localhost", user="klimasensor", passwd="klimasensor", db="klimasensordb")

cache = None
cacheFrom = None

if __name__ == '__main__':
    try:
        app.run(host='127.0.0.1', port=5000)
        app.config.update(
            DATABASE_URI='sqlite:///flask-openid.db',
            SECRET_KEY='development key',
            DEBUG=True
        )
    finally:
        db.close()