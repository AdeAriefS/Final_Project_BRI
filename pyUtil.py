import database
import base64
from datetime import datetime,timedelta,date

dbDNA = {
    'host':'172.18.62.38',
    'username':'dna',
    'password':'P@ssw0rd',
    'database':'DataAnalytics',
    'platform':'SQL Server'
}

def getColumnNamesFromTable(databaseName,tableName):
    selectQuery = f"SELECT COLUMN_NAME FROM [{databaseName}].[INFORMATION_SCHEMA].COLUMNS WHERE TABLE_CATALOG = '{databaseName}' AND TABLE_NAME = '{tableName}';"
    rows = database.selectData(selectQuery,dbDNA)
    return rows

def getConfig(key_name,key_group):
    sql = f"SELECT key_name, value, key_group FROM tbl_python_config WHERE key_name ='{key_name}' AND key_group = '{key_group}'"
    rows =  database.selectDataWithFieldNames(sql,dbDNA)
    config = {}
    for row in rows:
        config[row['key_name']] = row['value']
    return config



def getConfigByKeyGroup(key_group):
    sql = f"SELECT key_name, value, key_group FROM tbl_python_config WHERE key_group ='{key_group}'"
    rows =   database.selectDataWithFieldNames(sql,dbDNA)
    config = {}
    for row in rows:
        config[row['key_name']] = row['value']
    return config

def decodePassword(auth):
    return base64.b64decode(auth).decode('utf-8').split(':')

def convertValue(input):
    if not isinstance(input,str):
        if input != None:
            if str(input) == 'nan': return '0'
            # elif isinstance(input,datetime): return "'" + str(input) + "'"
            elif isinstance(input,datetime): 
                tempDate = datetime.strftime(input,'%Y-%m-%d %H:%M:%S')
                return f"IIF (ISDATE('{tempDate}') = 1,'{tempDate}',NULL)"
                # return "'" + datetime.strftime(input,'%Y-%m-%d %H:%M:%S') + "'"
            elif isinstance(input,date): 
                tempDate = datetime.strftime(input,'%Y-%m-%d')
                return f"IIF (ISDATE('{tempDate}') = 1,'{tempDate}',NULL)"
            elif isinstance(input,bool): return "'" + str(input) + "'"
            elif isinstance(input,timedelta): return "'" + str(input) + "'"
            elif isinstance(input,date): return "'" + str(input) + "'"
            elif isinstance(input,list): return "'" + str(input).replace("'","''")+ "'"
            elif isinstance(input,dict): return "'" + str(input).replace("'","''") + "'"
            else:
                return str(input)
        else : return 'NULL'
    else :
        if (len(input) > 50):
            return "'" + input[:50].replace("'","''") + "'"
        else : return "'" + input.replace("'","''") + "'"

def flatten_json_sepwith(y,sep):
    out = {}
 
    def flatten(x, name=''):
 
        # If the Nested key-value
        # pair is of dict type
        if type(x) is dict:
 
            for a in x:
                flatten(x[a], name + a + sep)
 
        # If the Nested key-value
        # pair is of list type
        elif type(x) is list:
 
            i = 0
 
            for a in x:
                flatten(a, name + str(i) + sep)
                i += 1
        else:
            out[name[:-1]] = x
 
    flatten(y)
    return out

def flatten_json(y):
    out = {}
 
    def flatten(x, name=''):
 
        # If the Nested key-value
        # pair is of dict type
        if type(x) is dict:
 
            for a in x:
                flatten(x[a], name + a + '.')
 
        # If the Nested key-value
        # pair is of list type
        elif type(x) is list:
 
            i = 0
 
            for a in x:
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x
 
    flatten(y)
    return out


def split_list(input_list, n):
    # Calculate the length of each sublist
    chunk_size = len(input_list) // n
    remainder = len(input_list) % n  # Calculate the remainder

    # Initialize a list of sublists
    sublists = []

    # Split the input list into sublists
    start = 0
    for i in range(n):
        if i < remainder:
            end = start + chunk_size + 1
        else:
            end = start + chunk_size
        sublist = input_list[start:end]
        sublists.append(sublist)
        start = end

    return sublists

