import sys 
import bluetooth 
import threading
import MySQLdb
import configparser

def format_row(row, type, i, rows):
	str_row = list(map(str, list(row)))
	pkg = type + ";" + str(i) + "/" + str(len(rows)) + ";" + ",".join(str_row)
	return pkg

class echoThread(threading.Thread):

    def getFile(self):
        f = open('timeseries.csv', 'r')
        content = f.read()
        answer = "filecontent;" + content
        self.sock.send(answer)

    def getFileInfo(self):
        f = open('timeseries.csv', 'r')
        content = f.read()
        print("size of content: " + str(len(content)))
        answer = "fileinfo;" + str(len(content))
        self.sock.send(answer)
        f.close()

    def getData(self):
        cur = self.db.cursor()
        cur.execute("SELECT * FROM data")
        rows = cur.fetchall()
        i = 1
        for row in rows:
            pkg = format_row(row, "d", i, rows)
            self.sock.send(pkg)
            resp = self.sock.recv(1024)
            print(self.client_info, ": sent [%s]" % pkg)
            if (resp == "ok"):
                i = i + 1
                continue
            else:
                break

    def getDataUpdate(self, data):
        splitData = data.split(";")
        fromTs = splitData[1]
        print("fetching entries since ", fromTs)
        cur = self.db.cursor()
        try:
            cur.execute("SELECT * FROM data WHERE Timestamp > %s", (str(fromTs),))
            rows = cur.fetchall()
            i = 1
            for row in rows:
                pkg = format_row(row, "u", i, rows)
                self.sock.send(pkg)
                resp = self.sock.recv(1024)
                print(self.client_info, ": sent [%s]" % pkg)
                if (resp == "ok"):
                    i = i + 1
                    continue
                else:
                    break

        except:
            return

    def __init__ (self,sock,client_info,db):
        threading.Thread.__init__(self)
        self.sock = sock
        self.client_info = client_info
        self.db = db

    def run(self):
        try:
            while True:
                print("listening for new requests")
                data = self.sock.recv(1024)
                if len(data) == 0: break
                print(self.client_info, ": received [%s]" % data)

                if data == "getFile":
                    self.getFile()

                if data == "getFileInfo":
                    self.getFileInfo()

                if data == "getData":
                    self.getData()

                if "getDataUpdate" in data:
                    self.getDataUpdate(data)

                else:
                    self.sock.send(data)
                    print(self.client_info, ": sent [%s]" % data)

        except IOError:
            pass

        self.sock.close()
        self.db.close()
        print(self.client_info, ": disconnected")


###############
# Main Method #
###############

def start():
    config = configparser.ConfigParser()
    config.read('config.ini')

    name = "BluetoothChat"
    uuid = config['Bluetooth']['uuid']

    server_sock=bluetooth.BluetoothSocket( bluetooth.RFCOMM )
    server_sock.bind(("",bluetooth.PORT_ANY))
    server_sock.listen(1)
    port = server_sock.getsockname()[1]

    bluetooth.advertise_service(server_sock, name, uuid)

    db = MySQLdb.connect(host="localhost",user="klimasensor",passwd="klimasensor",db="klimasensordb")

    while True:
        client_sock, client_info = server_sock.accept()
        print(client_info, ": connection accepted")
        db = MySQLdb.connect(host="localhost",user="klimasensor",passwd="klimasensor",db="klimasensordb")
        echo = echoThread(client_sock, client_info, db)
        echo.setDaemon(True)
        echo.start()

    server_sock.close()

