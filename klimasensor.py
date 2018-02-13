#!/usr/bin/python
import time
import datetime
import MySQLdb
from sense_hat import SenseHat

def writeToFile(timestamp, humidity, temp, pressure):
    with open("timeseries.csv", 'a') as f:
        f.write(str(timestamp))
        f.write(",")       
        f.write(str(humidity))
        f.write(",")
        f.write(str(temp))
        f.write(",")
        f.write(str(pressure))
        f.write("\n")

def showWaitingBar(sense,wait, timeInterval):
    percentage = wait / timeInterval
    numLEDs = int(round(64 * (1 - percentage)))
    X = [255, 0, 0] 
    O = [0, 0, 0]
    pixels = []
    for i in range(64):
	if i < numLEDs:
	    pixels.append(X)
	else:
	    pixels.append(O)

    sense.set_pixels(pixels)

def getSensorData(sense):
    data = {}
    data['humidity'] = sense.get_humidity()
    data['temp'] = sense.get_temperature()
    temp2 = sense.get_temperature_from_humidity()
    temp3 = sense.get_temperature_from_pressure()
    data['pressure'] = sense.get_pressure()

    print("Humidity: %s %%rH" % data['humidity'])
    print("Temperature: %s C" % data['temp'])
    print("Temperature from Humiditiy: %s C" % temp2)
    print("Temperature from Pressure: %s C" % temp3)
    print("Pressure: %s Millibars" % data['pressure'])

    return data

def showMeasurements(sense, data):
    temp = "{0:.1f} C".format(data['temp'])
    humid = "{0:.1f} %".format(data['humidity'])
    press = "{0:.1f} hP".format(data['pressure'])
    sense.show_message(temp)
    sense.show_message(humid)
    sense.show_message(press)

def checkDisplayEvent(sense):
    events = sense.stick.get_events()
    if not events:
	return
    else:
	showMeasurements(sense, data)
	return  

def getSecondsLeft(lastTs, timeInterval):
    ts = time.time()
    timeLeft = lastTs + timeInterval - ts
    return timeLeft

def indicateWaitingTime(timeInterval):
    lastTs = time.time()
    sense.clear()
    wait = timeInterval
    timeDelta = timeInterval / 64
    while wait > 0:
        showWaitingBar(sense, wait, timeInterval)
        time.sleep(1)
	checkDisplayEvent(sense)
	wait = getSecondsLeft(lastTs, timeInterval)
        

def writeToDb(db, data):
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO data (Humidity,Temperature,Pressure) VALUES (%s,%s,%s)", (data['humidity'],data['temp'],data['pressure']))
        db.commit()
    except:
        db.rollback()



def readFromDb(db):
    cursor = db.cursor()
    try: 
        cursor.execute("SELECT * FROM data")
      	return cursor.fetchall()
    except:
	print "could not fetch entries" 

############
# Main method #
############

# time interval between measurements
timeInterval = 10*60
# timeInterval = 30

sense = SenseHat()
sense.clear()
sense.low_light = True
# writeToFile('timestamp', 'humidity', 'temp', 'pressure')

db = MySQLdb.connect(host="localhost",user="klimasensor",passwd="klimasensor",db="klimasensordb")


while True:
    data = getSensorData(sense)
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
#    writeToFile(st, data['humidity'], data['temp'], data['pressure'])
    writeToDb(db, data)
 
    indicateWaitingTime(timeInterval)

    


