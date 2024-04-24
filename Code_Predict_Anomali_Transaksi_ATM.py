import pandas as pd 
from prophet import Prophet
import cmdstanpy
import database
from datetime import datetime, timedelta
import math 
import myLogger
import pyUtil
import pyodbc

dbATM = {
    'host': '172.18.61.214',
    'username': 'dta_python',
    'password': 'an4Lytics!@#',
    'database': 'PROSWITCHING',
    'platform': 'MySQL'
}

dbDNA = {
    'host': '172.18.62.38',
    'username': 'dna',
    'password': 'P@ssw0rd',
    'database': 'PROSWITCHING',
    'platform': 'SQL Server'
}

loggerName = 'ATMTrx30min'
delta = 30
def round_up_to_last_minutes(dt):
    # Calculate the number of minutes past the last 30-minute mark
	minutes_past_last = dt.minute % delta

    # If minutes_past_last_30 is non-zero, round up; otherwise, leave as is
	if minutes_past_last != 0:
		dt -= timedelta(minutes=minutes_past_last)

    # Set the seconds and microseconds to zero
	dt = dt.replace(second=0, microsecond=0)
	return dt

def generateInsertQuery(row,databaseName, tableName,colInfo):
    columnString = ''
    valueString = ''
    # columns = pyUtil.getColumnNamesFromTable(databaseName,tableName)
    # for key in row:
    for column in colInfo:
        key = column[0]
        if key in row:
            columnString += '['+ key + '],'
            valueString += pyUtil.convertValue(row[key]) + ','
            # myLogger.logging_info(
            #         'engineImportEDCKonven15Minutes', 
            #         'columnString:', key,
            #         ',valueString:',row[key],',type:',type(row[key]))
        
    sqlInsert = f'INSERT INTO [{databaseName}].dbo.[{tableName}] ({columnString[0:len(columnString)-1]}) VALUES ({valueString[0:len(valueString)-1]})'
    return sqlInsert

def main():
    # n_days_ago = 3
    # tanggalAwal = datetime.strftime(round_up_to_last_minutes(datetime.now()),'%Y-%m-%d %H:%M:%S')
    tanggalAkhir = datetime.strftime(round_up_to_last_minutes(datetime.now()),'%Y-%m-%d %H:%M:%S')
    futureList = [datetime.strftime(round_up_to_last_minutes(datetime.now()+timedelta(minutes=delta)),'%Y-%m-%d %H:%M:%S'), datetime.strftime(round_up_to_last_minutes(datetime.now()+timedelta(minutes=delta*2)),'%Y-%m-%d %H:%M:%S')]
    # future = pd.Series(futureList,name='ds').to_frame()
    future = pd.date_range(start=tanggalAkhir[0:10]+' 00:00:00', end=datetime.strftime(round_up_to_last_minutes(datetime.now()+timedelta(minutes=delta)),'%Y-%m-%d %H:%M:%S'), freq='30T').to_frame(name='ds')
    # future = pd.date_range(start='2023-10-28 00:00:00', end=datetime.strftime(round_up_to_last_minutes(datetime.now()+timedelta(minutes=delta)),'%Y-%m-%d %H:%M:%S'), freq='30T').to_frame(name='ds')
    # myLogger.logging_info(loggerName,'future : ',future)

    # tanggalAkhir = f"{datetime.strftime(datetime.now(),'%Y-%m-%d %H:')}{math.floor(int(datetime.strftime(datetime.now() , '%M'))/30)*30}:00"
    
    #select table realtime group 30min
    queryGroupingAtm = f'''
    SELECT 
        CONCAT( DATE_FORMAT(a.Tanggal, '%Y-%m-%d %H:'),LPAD( CONVERT(FLOOR(EXTRACT(MINUTE FROM 
        a.Tanggal)/{delta}) * {delta}, CHAR(2)),2,'0'),':00') AS trx_date
        , fitur
        , isocode
        , responcode
        , COUNT(1) total_trx
        , sum(amount) total_amount
        , case when responcode = '00' then 'sukses'
            when responcode in ('Q1', 'Q2', 'Q4', 'Q6', '68', '91', '92') then 'gagal_sistem'
            else 'gagal_bisnis' end as is_success
    FROM atm_transaction a
    WHERE Tanggal >= '{tanggalAkhir[0:10]}'
    and Tanggal <= '{tanggalAkhir}'
    and isocode = 210
    GROUP BY 
        CONCAT(DATE_FORMAT(a.Tanggal, '%Y-%m-%d %H:'),LPAD( CONVERT(FLOOR(EXTRACT(MINUTE FROM 
        a.Tanggal)/{delta}) * {delta}, CHAR(2)),2,'0'),':00')
        , fitur
        , isocode
        , responcode
        , case when responcode = '00' then 'sukses' 
            when responcode in ('Q1', 'Q2', 'Q4', 'Q6', '68', '91', '92') then 'gagal_sistem'
            else 'gagal_bisnis' end
    ORDER BY trx_date
    '''
    # print(tanggalAkhir)
    # print(tanggalAkhir[0:10])
    # print(queryGroupingIncMc)
    myLogger.logging_info(loggerName,queryGroupingAtm)
    try:
        rowsGroupingAtm = database.selectDataWithFieldNames(queryGroupingAtm, dbATM)
    except Exception as e:
        myLogger.logging_error(loggerName, 'Select Grouping Failed - Exception:', e)
        return

    if len(rowsGroupingAtm) > 0:
        prevent_duplicate = f'''TRUNCATE TABLE PROSWITCHING.dbo.atm_transaction_realtime_grouping'''
        try:
            database.executeQuery(prevent_duplicate, dbDNA)
        except Exception as e:
             myLogger.logging_error(loggerName, 'Truncate Fail - Exception:', e)
            # print('Exception:', e) 
    
    #insert realtime data 
    tblColInfos = pyUtil.getColumnNamesFromTable('PROSWITCHING', 'atm_transaction_realtime_grouping')
    iterInsert = 0
    totalInsertSuccess = 0
    listInsertQuery = []
    for row in rowsGroupingAtm:
        iterInsert += 1
        try:
            insertQuery = generateInsertQuery(row, 'PROSWITCHING', 'atm_transaction_realtime_grouping',tblColInfos)
            listInsertQuery.append(insertQuery)
            totalInsertSuccess += 1
        except Exception as e:
            # print('Exception')
            myLogger.logging_error(loggerName, 'got exception:', e)
            myLogger.logging_error(loggerName, 'insertQuery:', insertQuery)
        
    myLogger.logging_info(loggerName, 'total query to atm_transaction_realtime_grouping:', len(listInsertQuery))
    try:
        totalInsertSuccess = database.executeQueryList(listInsertQuery,dbDNA,100)
    except Exception as e:
        myLogger.logging_error(loggerName, 'got exception:', e)
        #myLogger.logging_error(loggerName, 'query:, ',listInsertQuery)
        print(listInsertQuery) 

    #check if need to select data H-1, in case the job daily python H-1 has not yet running
    queryCheckH1 = f'''
    with a as (
    select  count(1) total_in_summary_daily from dbo.atm_transaction_summary_daily_per30m where 
    convert(varchar(10),trx_date,120) = convert(varchar(10), DATEADD(DAY,-1,'{tanggalAkhir[0:10]}'),120) and isocode = 210
    ),
    b as (
    select  count(1) total_in_summary_h1 from dbo.atm_transaction_summary_daily_H1 where 
    convert(varchar(10),trx_date,120) = convert(varchar(10), DATEADD(DAY,-1,'{tanggalAkhir[0:10]}'),120) and isocode = 210
    )
    select * from a
    cross join b
    '''
    myLogger.logging_info(loggerName,'queryCheckH1: \n',queryCheckH1)
    try:
        rowsCheckH1 = database.selectDataWithFieldNames(queryCheckH1, dbDNA)
    except Exception as e:
        myLogger.logging_info(loggerName, 'Select Grouping Failed - Exception:', e)
        return
    
    total_in_summary_daily = rowsCheckH1[0]['total_in_summary_daily']
    total_in_summary_h1 = rowsCheckH1[0]['total_in_summary_h1']
    myLogger.logging_info(loggerName,f'total_in_summary_daily:{total_in_summary_daily},total_in_summary_h1:{total_in_summary_h1}')
    if total_in_summary_daily == 0 and total_in_summary_h1 == 0 :
        queryGroupingAtmH1 = f'''
        SELECT 
            CONCAT( DATE_FORMAT(a.Tanggal, '%Y-%m-%d %H:'),LPAD( CONVERT(FLOOR(EXTRACT(MINUTE FROM 
            a.Tanggal)/{delta}) * {delta}, CHAR(2)),2,'0'),':00') AS trx_date
            , fitur
            , isocode
            , responcode
            , COUNT(1) total_trx
            , sum(amount) total_amount
            , case when responcode = '00' then 'sukses' 
			    when responcode in ('Q1', 'Q2', 'Q4', 'Q6', '68', '91', '92') then 'gagal_sistem'
			    else 'gagal_bisnis' end as is_success
        FROM atm_transaction a
        WHERE Tanggal >= DATE_ADD('{tanggalAkhir[0:10]}',INTERVAL -1 DAY)
        and Tanggal < '{tanggalAkhir[0:10]}'
        and isocode = 210
        GROUP BY 
            CONCAT( DATE_FORMAT(a.Tanggal, '%Y-%m-%d %H:'),LPAD( CONVERT(FLOOR(EXTRACT(MINUTE FROM 
            a.Tanggal)/{delta}) * {delta}, CHAR(2)),2,'0'),':00')
            , fitur
            , isocode
            , responcode
            , case when responcode = '00' then 'sukses' 
			    when responcode in ('Q1', 'Q2', 'Q4', 'Q6', '68', '91', '92') then 'gagal_sistem'
			    else 'gagal_bisnis' end
        ORDER BY trx_date
        '''
        myLogger.logging_info(loggerName,'queryGroupingAtmH1 : ',queryGroupingAtmH1)
        try:
            rowsGroupingAtmH1 = database.selectDataWithFieldNames(queryGroupingAtmH1, dbATM)
        except Exception as e:
            myLogger.logging_info(loggerName, 'Select Grouping H1 Failed - Exception:', e)
        

        if len(rowsGroupingAtmH1) > 0:
            prevent_duplicate = f'''TRUNCATE TABLE PROSWITCHING.dbo.atm_transaction_summary_daily_H1'''
            try:
                database.executeQuery(prevent_duplicate, dbDNA)
            except Exception as e:
                myLogger.logging_info(loggerName, 'Truncate H1 Fail - Exception:', e)
                # print('Exception:', e) 
        
        #insert realtime data 
        tblColInfos = pyUtil.getColumnNamesFromTable('PROSWITCHING', 'atm_transaction_summary_daily_H1')
        iterInsert = 0
        totalInsertSuccess = 0
        listInsertQuery = []
        for row in rowsGroupingAtm:
            iterInsert += 1
            try:
                insertQuery = generateInsertQuery(row, 'PROSWITCHING', 'atm_transaction_summary_daily_H1',tblColInfos)
                listInsertQuery.append(insertQuery)
                totalInsertSuccess += 1
            except Exception as e:
                # print('Exception')
                myLogger.logging_error(loggerName, 'got exception:', e)
                myLogger.logging_error(loggerName, 'insertQuery:', insertQuery)
            
        myLogger.logging_info(loggerName, 'total query to atm_transaction_summary_daily_H1:', len(listInsertQuery))
        try:
            totalInsertSuccess = database.executeQueryList(listInsertQuery,dbDNA,100)
        except Exception as e: 
            myLogger.logging_error(loggerName, 'got exception:', e)
            myLogger.logging_error(loggerName, 'query:, ',listInsertQuery)
        
    
    queryCfg = "SELECT * FROM [DataAnalytics].dbo.tbl_atm_prediction WHERE is_active = 'Y'"
    try:
        rowsCfg = database.selectDataWithFieldNames(queryCfg,dbDNA)
    except Exception as e:
        myLogger.logging_error(loggerName,'fail to get config data, excp: ',e)
    for config in rowsCfg:
        config['is_success_ori'] = config['is_success'] 
        # config['is_success'] = "= '00'" if config['is_success'] == 'Y' else "<> '00'"
        config['total_in_summary_daily'] = total_in_summary_daily
        config['total_in_summary_h1'] = total_in_summary_h1
        query = config['query'].format(**config)
        # print(query)
        myLogger.logging_info(loggerName,f"config - {config['predict_desc']}:",config)
        myLogger.logging_info(loggerName,f"query - {config['predict_desc']}:",query)
        try:
            conn = pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=172.18.62.38;'
        'PORT=1433;'
        'UID=dna;'
        'PWD=P@ssw0rd;'
        'Database=PROSWITCHING;')
            df = pd.read_sql_query(query,conn)
        except Exception as e:
            myLogger.logging_info(loggerName,f"failed to query - {config['predict_desc']}, excp :",e)
            break
        
        dsColumn = config['column_ds']
        listColumnToPredict = eval(config['column_to_predict'])
        df_res_all = pd.DataFrame()
        df_res_list = []
        for colToPredict in listColumnToPredict:
            df_temp =  df[[dsColumn,colToPredict]].rename(columns={dsColumn:'ds', colToPredict: 'y'})
            myLogger.logging_info(loggerName,f"df_temp - {config['predict_desc']} {colToPredict} \n :",df_temp.to_string())

            #Generate Model
            model = Prophet()
            model.fit(df_temp)

            #forecast
            forecast = model.predict(future)
            # Mengganti nilai negatif dengan nol
            forecast['yhat'] = forecast['yhat'].apply(lambda x: max(0, x)) #<==
            #Melakukan Penghapusan data dengan nilai nol
            forecast = forecast[forecast['yhat'] != 0] #<==
            myLogger.logging_info(loggerName,f"future:",forecast)
            df_res = forecast[['ds','yhat']].rename(columns={
                'ds' : dsColumn,
                'yhat' : colToPredict
            })
            myLogger.logging_info(loggerName,f"df_res - {config['predict_desc']} {colToPredict} \n :",df_res.to_string())

            df_res_list.append(df_res)
        
        for df_res in df_res_list:
            if df_res_all.empty:
                df_res_all = df_res
            else:
                df_res_all  = pd.merge(df_res_all,df_res, on = dsColumn, how='inner')
        # myLogger.logging_info(loggerName,'df_res_all.head() : ',df_res_all.head())
        listQuery = []
        for index,row in df_res_all.iterrows():
            colStr = '''
            [trx_date]
           ,[fitur]
           ,[isocode]
           ,[is_success]
           '''
            valStr = f'''
            '{row[dsColumn]}'
            ,'{config['fitur']}'
            ,'{config['isocode']}'
            ,'{config['is_success_ori']}'
            '''
            for colToPredict in listColumnToPredict:
                colStr += f'''
                ,[{colToPredict}]
                '''
                valStr += f'''
                ,{row[colToPredict]}
                '''
            queryInsertWithDelete = f'''
            DELETE FROM PROSWITCHING.dbo.atm_transaction_predict_new
            WHERE trx_date = '{row[dsColumn]}'
            AND [fitur] = '{config['fitur']}' 
            AND [is_success] = '{config['is_success_ori']}';
            INSERT INTO PROSWITCHING.dbo.atm_transaction_predict_new
            ({colStr}) VALUES ({valStr})
            '''
            myLogger.logging_info(loggerName,'insert query : ',queryInsertWithDelete)
            listQuery.append(queryInsertWithDelete)
        try:
            database.executeQueryList(listQuery,dbDNA,1)
        except Exception as e:
            myLogger.logging_error(loggerName,'failed to insert data, excp:',e)

if __name__ == "__main__":
    main()
