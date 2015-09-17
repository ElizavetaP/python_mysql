import MySQLdb
import sys
import ConfigParser
import time
from datetime import datetime


config = ConfigParser.RawConfigParser()
config.read('configs.ini')

try:
    db = MySQLdb.connect(host=config.get('mysql', 'host'), user=config.get('mysql', 'user'), passwd = config.get('mysql', 'passwd'),
                         db = config.get('mysql', 'db'))
    
except Exception:
    print('Access denied for user meteouser@localhost (using password: YES)')
    sys.exit(1)
    
cursor = db.cursor()

def convert(T):
    if config.get('temperature', 'T')== 'F':
        T = float(5)/9*(T - 32)
    return round(T,5)

def revconv (T):
    if config.get('temperature', 'T')== 'F':
        T = T*float(9)/5 + 32
    return round(T)

if len(sys.argv)==1 or sys.argv[1] not in ['import', 'site', 'average', 'daycalc', 'hottest', 'all', 'history']:
    print('select mode: import, site, average, daycalc, hottest, all, history')
    sys.exit(1)

if sys.argv[1] == 'import':
    if len(sys.argv)==4:
        
        def unpack_line(line):
            argv = line.split(';')
            return argv

        f = open(sys.argv[3], "r")
        lines = f.readlines()

        for line in lines:
            argv = unpack_line(line)
            sql = """INSERT INTO prognoz3(date, temperature, pressure, nebulosity, humidity, station)
            VALUES ('%(date)s', '%(temperature)s', '%(pressure)s', '%(nebulosity)s', '%(humidity)s', '%(station)s')
            """%{"date":argv[0], "temperature":convert(float(argv[1])), "pressure":argv[2], "nebulosity":argv[3], "humidity":argv[4], "station":sys.argv[2]}
            cursor.execute(sql)
            db.commit()

    else:
        print("Write: mode station filename")
        sys.exit(1)

if sys.argv[1] == 'site':
    import urllib2
    import json
    response = urllib2.urlopen('http://api.openweathermap.org/data/2.5/forecast/daily?q=Moscow,200&cnt=1')
    data = response.read()
    a = json.loads(data)
    sql = """INSERT INTO prognoz3(date, temperature, pressure, nebulosity, humidity, station)
    VALUES (CAST(sysdate() AS DATE), '%(temperature)s', '%(pressure)s', '%(nebulosity)s', '%(humidity)s', 'site')
    """%{"temperature":a['list'][0]['temp']['day']-273.15 , "pressure":a['list'][0]['pressure'], "nebulosity":a['list'][0]['clouds'], "humidity":a['list'][0]['humidity']}
    cursor.execute(sql)
    db.commit()

    print 'station', 'site'
    d = datetime.today()
    print 'date', d.date()
    print 'temperature', a['list'][0]['temp']['day'], 'K'
    print 'pressure', a['list'][0]['pressure']
    print 'nebulosity', a['list'][0]['clouds']
    print 'humidity', a['list'][0]['humidity']

def average(data):
    t = 0
    n = 0
    for rec in data:
        t = t + float(rec[0])
        n = n + 1
    return (t/n)
    
if sys.argv[1] == 'average':
    if len(sys.argv)==4:
        sql2 = """SELECT temperature from prognoz3 WHERE date BETWEEN '%(1)s' AND '%(2)s'
        """%{"1":sys.argv[2], "2":sys.argv[3]}
        cursor.execute(sql2)
        data = cursor.fetchall()

        print 'average temperature:', int(revconv(average(data))), config.get('temperature', 'T')

    else:
        print("Write: mode first date last date")
        sys.exit(1)

########average for day###############
def daycalc(st,param):
    
    sql3 = """SELECT %(1)s from prognoz3 WHERE station = '%(2)s' AND date = CAST(sysdate() AS DATE) 
    """%{"1":param, "2":st}
    cursor.execute(sql3)

    data = cursor.fetchall()
    
    if param == 'temperature':
        return int(revconv(average(data)))
    else:
        return int(average(data))
######################################

if sys.argv[1] == 'daycalc':
    if len(sys.argv)==4:
        if sys.argv[3] == 'temperature':
            print 'day-calculate for a station', daycalc(sys.argv[2],sys.argv[3]), config.get('temperature', 'T')
        else:
            print 'day-calculate for a station', daycalc(sys.argv[2],sys.argv[3])
    else:
        print("Write: mode station parameter")
        sys.exit(1)
        
##########print all###################
def allst(data):
    allst = []
    for rec in data:
        if rec[0] not in allst:
            allst.append(rec[0])
    return allst
            
def printall(st):
    sql5 = """SELECT * from prognoz3 WHERE station = '%(1)s' AND date = CAST(sysdate() AS DATE) 
    """%{"1":st}
    cursor.execute(sql5)

    data = cursor.fetchall()
    for rec in data:
        print 'station', rec[5]
        print 'date', rec[0]
        print 'temperature', int(revconv(float(rec[1]))), config.get('temperature', 'T')
        print 'pressure', rec[2]
        print 'nebulosity', rec[3]
        print 'humidity', rec[4]
        print
#########################################
        
if sys.argv[1] == 'hottest':

    t = -100
    sql4 = """SELECT station from prognoz3 WHERE date = CAST(sysdate() AS DATE) 
    """
    cursor.execute(sql4)
    data = cursor.fetchall()
    maxst = None
    for st in allst(data):
        tst = daycalc(st,'temperature')
        if tst > t:
            maxst = st
            t = tst
    if maxst == None:
        print 'There is no data today'
    else:
        print 'hottest satation:', maxst
    printall(maxst)

if sys.argv[1] == 'all':
    sql4 = """SELECT station from prognoz3 WHERE date = CAST(sysdate() AS DATE) 
    """
    cursor.execute(sql4)
    data = cursor.fetchall()
    for st in allst(data):
        printall(st)
    
if sys.argv[1] == 'history':

    sql = """select max(temperature), substring(date, 1, 7), station from prognoz3 group by substring(date, 1, 7), station
    """
    cursor.execute(sql)
    data = cursor.fetchall()
    print '{0:10} | {1:10} | {2:10}'.format('date', 'station', 'temperature')
    for rec in data:
        print '{0:10} | {1:10} | {2:10}'.format(str(rec[1]), str(rec[2]), str(int(revconv(float(rec[0])))) + '  C')
   


db.close()
