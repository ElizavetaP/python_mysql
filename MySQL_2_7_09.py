import MySQLdb
import sys


if not len(sys.argv)==3:
    print("Write: station filename")
    sys.exit(1)
    
db = MySQLdb.connect(host="localhost", user="root", passwd = "surf", db = "db")
cursor = db.cursor()
    
if sys.argv[2] == 'None':
    a = []
    while len(a) != 5:
        a = input('Write: date temperature pressure nebulosity humidity:  ')
        a = a.split()
    argv = a
    
    print argv
        


    sql = """INSERT INTO prognoz1(date, temperature, pressure, nebulosity, humidity, station)
    VALUES ('%(date)s', '%(temperature)s', '%(pressure)s', '%(nebulosity)s', '%(humidity)s', '%(station)s')
    """%{"date":argv[0], "temperature":argv[1], "pressure":argv[2], "nebulosity":argv[3], "humidity":argv[4], "station":sys.argv[1]}
    cursor.execute(sql)
    db.commit()
    
else:
    def unpack_line(line):
        argv = line.split(';')
        return argv

    f = open(sys.argv[1], "r")
    lines = f.readlines()

    for line in lines:
        argv = unpack_line(line)
        sql = """INSERT INTO prognoz1(date, temperature, pressure, nebulosity, humidity, station)
        VALUES ('%(date)s', '%(temperature)s', '%(pressure)s', '%(nebulosity)s', '%(humidity)s', '%(station)s')
        """%{"date":argv[0], "temperature":argv[1], "pressure":argv[2], "nebulosity":argv[3], "humidity":argv[4], "station":sys.argv[1]}
        cursor.execute(sql)
        db.commit()
    

##print('avarage temperature')
##first = input('first date: ')
##last = input('last date: ')
##sql2 = """SELECT temperature from prognoz1 WHERE date BETWEEN '%(1)s' AND '%(2)s'
##"""%{"1":first, "2":last}
##cursor.execute(sql2)
##
##data = cursor.fetchall()
##print(data)
##
##t = 0
##n = 0
##
##for rec in data:
##    t = t + int(rec[0])
##    n = n + 1
##print(t/n)


print('day-calculate for a station')
st = input('station name: ')
param = input('parameter: ')
sql3 = """SELECT %(1)s from prognoz1 WHERE station = '%(2)s' AND date = CAST(sysdate() AS DATE) 
"""%{"1":param, "2":st}
cursor.execute(sql3)
print sql3

data = cursor.fetchall()
print(data)

t = 0
n = 0

for rec in data:
    t = t + int(rec[0])
    n = n + 1
print(t/n)


db.close()
