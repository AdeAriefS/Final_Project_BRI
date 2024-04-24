import pyodbc
import mysql.connector
import platform
import copy
import cx_Oracle
import psycopg2
import myLogger
dbDna = {
    'host':'172.18.62.38',
    'username':'dna',
    'password':'P@ssw0rd',
    'database':'DataAnalytics',
    'platform':'SQL Server'
}

def getDriverForSqlServer():
    if (platform.system() == 'Linux'):
        return '{ODBC Driver 18 for SQL Server}'
    else:
        return '{SQL Server}'

def genConnString(dbConn):
    if dbConn['platform'] == 'SQL Server':
        map = {
            'host':'SERVER',
            'username':'UID',
            'password':'PWD',
            'database':'DATABASE'
        }
        strDbConn = 'DRIVER={};'.format(getDriverForSqlServer())
    elif dbConn['platform'] == 'MySQL':
        map = {}
        strDbConn = ''

    for key in dbConn:
        if key == 'platform':
            continue
        param = key if key not in map else map[key]
        value = dbConn[key]
        strDbConn += '{}={};'.format(param,value)
    # print(strDbConn)
    return strDbConn

def selectData(query,dbConn):
    if(dbConn['platform'] == 'SQL Server'):
        with pyodbc.connect(genConnString(dbConn)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            return rows
    elif (dbConn['platform'] == 'MySQL'):
        del dbConn['platform']
        # with mysql.connector.connect(host=dbConn["host"],database=dbConn["database"],user=dbConn["username"],password=dbConn["password"]) as conn:
        with mysql.connector.connect(**dbConn) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows

def selectDataAndFieldNames(query,conn):
    dbConn = copy.deepcopy(conn)
    if(dbConn['platform'] == 'SQL Server'):
        with pyodbc.connect(genConnString(dbConn)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            return rows, field_names
    elif (dbConn['platform'] == 'MySQL'):
        del dbConn['platform']
        # with mysql.connector.connect(host=dbConn["host"],database=dbConn["database"],user=dbConn["username"],password=dbConn["password"]) as conn:
        with mysql.connector.connect(**dbConn) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            return rows, field_names

def selectDataWithFieldNames(query,conn,multi=False):
    dbConn = copy.deepcopy(conn)
    if(dbConn['platform'] == 'SQL Server'):
        with pyodbc.connect(genConnString(dbConn)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            rowsWithFieldNames = [dict(zip(field_names,row)) for row in rows]
            return rowsWithFieldNames
    elif (dbConn['platform'] == 'MySQL'):
        del dbConn['platform']
        # with mysql.connector.connect(host=dbConn["host"],database=dbConn["database"],user=dbConn["username"],password=dbConn["password"]) as conn:
        with mysql.connector.connect(**dbConn) as conn:
            cursor = conn.cursor()
            if not multi:
                cursor.execute(query)
                field_names = [i[0] for i in cursor.description]
                rows = cursor.fetchall()
                cursor.close()
                rowsWithFieldNames = [dict(zip(field_names,row)) for row in rows]
            else:
                results = cursor.execute(query,multi=True)
                for result in results:
                    if result.with_rows:
                        field_names = [i[0] for i in result.description]
                        rows = result.fetchall()
                        rowsWithFieldNames = [dict(zip(field_names,row)) for row in rows]
            return rowsWithFieldNames
    elif(dbConn['platform']) == 'Oracle':
        dsn_db = cx_Oracle.makedsn(dbConn['host'], dbConn['port'], sid=dbConn['sid'])
        with cx_Oracle.connect(user=dbConn['username'], password=dbConn['password'], dsn=dsn_db, encoding="UTF-8") as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            rowsWithFieldNames = [dict(zip(field_names,row)) for row in rows]
            return rowsWithFieldNames
    elif(dbConn['platform']) == 'Postgre':
        with psycopg2.connect(host=dbConn['host'],database= dbConn["database"],user=dbConn['username'],password=dbConn['password']) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            rowsWithFieldNames = [dict(zip(field_names,row)) for row in rows]
            return rowsWithFieldNames

def executeQuery(query,dbConn):
    retval = 0
    if(dbConn['platform'] == 'SQL Server'):
        with pyodbc.connect(genConnString(dbConn)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.commit()
            retval = cursor.rowcount
            cursor.close()
    return retval

def executeQueryList(listQuery,dbConn,step):
    retval = 0
    if(dbConn['platform'] == 'SQL Server'):
        with pyodbc.connect(genConnString(dbConn)) as conn:
            cursor = conn.cursor()
            for i in range(0,len(listQuery),step):
                for y in range(i,i+len(listQuery[i:i+step])):
                    query = listQuery[y]
                    try:
                        cursor.execute(query)
                        retval += cursor.rowcount
                    except Exception as e:
                        myLogger.logging_error('exec fail',f'query:{query}')
                        myLogger.logging_error('exec fail',f'excep:{e}')
                cursor.commit()
            cursor.close()
        return retval



if __name__ == "__main__":
    query = "SELECT key_name,value FROM tbl_python_config WHERE KEY_NAME = 'URL_ELASTIC_BRIMO_DC'"
    print(dict(selectData(query,dbDna)))
