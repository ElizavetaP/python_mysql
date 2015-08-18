import MySQLdb
import sys
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('configs.ini')

db = MySQLdb.connect(host=config.get('mysql', 'host'), user=config.get('mysql', 'user'), passwd = config.get('mysql', 'passwd'),
                     db = config.get('mysql', 'db'))
cursor = db.cursor()

def convert(T):
    if config.get('temperature', 'T')== 'F':
        T = float(5)/9*(T - 32)
    return round(T,5)

def revconv (T):
    if config.get('temperature', 'T')== 'F':
        T = T*float(9)/5 + 32
    return round(T)

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
        print("Write: regime station filename")
        sys.exit(1)

if sys.argv[1] == 'avarage':
    if len(sys.argv)==4:
        sql2 = """SELECT temperature from prognoz3 WHERE date BETWEEN '%(1)s' AND '%(2)s'
        """%{"1":sys.argv[2], "2":sys.argv[3]}
        cursor.execute(sql2)
        data = cursor.fetchall()

        t = 0
        n = 0

        for rec in data:
            t = t + float(rec[0])
            n = n + 1
        print 'avarage temperature:', int(revconv(t/n))

    else:
        print("Write: regime first date last date")
        sys.exit(1)

########avarage for day###############
def daycalc(st,param):
    
    sql3 = """SELECT %(1)s from prognoz3 WHERE station = '%(2)s' AND date = CAST(sysdate() AS DATE) 
    """%{"1":param, "2":st}
    cursor.execute(sql3)

    data = cursor.fetchall()
    t = 0
    n = 0

    for rec in data:
        t = t + float(rec[0])
        n = n + 1
    if param == 'temperature':
        return int(revconv(t/n))
    else:
        return int(t/n)
######################################

if sys.argv[1] == 'daycalc':
    if len(sys.argv)==4:
        print 'day-calculate for a station', daycalc(sys.argv[2],sys.argv[3])
    else:
        print("Write: regime station parameter")
        sys.exit(1)
        
##########print all###################
def allst():
    sql4 = """SELECT station from prognoz3 WHERE date = CAST(sysdate() AS DATE) 
    """
    cursor.execute(sql4)
    data = cursor.fetchall()
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
        print 'temperature', int(revconv(float(rec[1])))
        print 'pressure', rec[2]
        print 'nebulosity', rec[3]
        print 'humidity', rec[4]
        print
#########################################
        
if sys.argv[1] == 'hottest':

    t = -100
    for st in allst():
        tst = daycalc(st,'temperature')
        if tst > t:
            maxst = st
            t = tst
    print 'hottest satation:', maxst
    printall(maxst)

if sys.argv[1] == 'all':
    for st in allst():
        printall(st)



db.close()
