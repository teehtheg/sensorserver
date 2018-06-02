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

class SensorDataCount(Resource):
    @requires_auth
    def get(self):
        cur = db.cursor()
        cur.execute("SELECT count(*) FROM data")
        try:
            num = cur.fetchone()
            return jsonify(count=num[0])

        except Exception as e:
            print(e)
            return

class SensorData(Resource):
    @requires_auth
    def get(self, pageNr=0):
        if (not pageNr or pageNr <= 0):
            pageNr = 0

        fromId = int(pageNr) * MAX_PAGE
        toId = fromId + MAX_PAGE - 1
        cur = db.cursor()
        cur.execute("SELECT * FROM data WHERE id BETWEEN %s AND %s", (fromId, toId))

        try:
            rows = cur.fetchall()
            result = [SensorRow(row).serialize() for row in rows]
            print("Sending " + str(len(result)) + " records")

            if (not rows or len(rows) < MAX_PAGE):
                return jsonify(Response(result).serialize())
            else:
                return jsonify(Response(result, int(pageNr) + 1).serialize())

        except Exception as e:
            print(e)
            return

class SensorDataFromCount(Resource):
    @requires_auth
    def get(self, fromTs):
        cur = db.cursor()
        cur.execute("SELECT count(*) FROM data WHERE Timestamp > %s", (fromTs,))
        try:
            num = cur.fetchone()
            return jsonify(count=num[0])

        except Exception as e:
            print(e)
            return

class SensorDataFrom(Resource):
    @requires_auth
    def get(self, fromTs, pageNr=0):
        if (not pageNr or pageNr <= 0):
            pageNr = 0

        cur = db.cursor()
        cur.execute("SELECT * FROM data WHERE Timestamp > %s", (fromTs,))

        try:
            row = cur.fetchone()
            startId = SensorRow(row).id
            fromId = startId + int(pageNr) * MAX_PAGE
            toId = fromId + MAX_PAGE - 1

            cur = db.cursor()
            cur.execute("SELECT * FROM data WHERE Timestamp > %s AND id BETWEEN %s AND %s", (fromTs,fromId,toId))

            rows = cur.fetchall()
            result = [SensorRow(row).serialize() for row in rows]
            print("Sending " + str(len(result)) + " records")

            if (not rows or len(rows) < MAX_PAGE):
                return jsonify(Response(result).serialize())
            else:
                return jsonify(Response(result, int(pageNr) + 1).serialize())

        except Exception as e:
            print(e)
            return


class Response:
    data = None
    next = None

    def __init__(self, data, next=None):
        self.data = data
        self.next = next

    def serialize(self):
        if (not next):
            return {
                'data': self.data
            }
        else:
            return {
                'data': self.data,
                'next': self.next
            }


class SensorRow:
    timestamp = None
    pressure = None
    temperature = None
    humidity = None
    id = None

    def __init__(self, array):
        if (len(array) >= 5):
            self.timestamp = array[0]
            self.humidity = array[1]
            self.temperature = array[2]
            self.pressure = array[3]
            self.id = array[4]

    def serialize(self):
        return {
            'id': self.id,
            'timestamp': self.timestamp,
            'humidity': self.humidity,
            'temperature': self.temperature,
            'pressure': self.pressure,
        }

app = Flask(__name__)
api = Api(app)

config = configparser.ConfigParser()
config.read('config.ini')

api.add_resource(Status,
                 '/status')
api.add_resource(SensorDataCount,
                 '/sensordata/count')
api.add_resource(SensorData,
                 '/sensordata/<pageNr>')
api.add_resource(SensorDataFromCount,
                 '/sensordatafrom/<fromTs>/count')
api.add_resource(SensorDataFrom,
                 '/sensordatafrom/<fromTs>/<pageNr>')

MAX_PAGE = 100
db = MySQLdb.connect(host="localhost", user="klimasensor", passwd="klimasensor", db="klimasensordb")

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