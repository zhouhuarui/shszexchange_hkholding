import pandas as pd
import pandas.io.sql as sql
import pymysql
db = pymysql.connect("localhost", "root", "myangelxjl","hkholding" )
cursor = db.cursor()
import requests

def szDataDownload(dataDate):

    #读取深交所数据文件，返回文件
    #若需要下载至本地请读取返回f.content

    #下载地址
    fileAddress1 = "http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx"
    fileAddress2 = "&CATALOGID=SGT_SGTCGSL&TABKEY=tab1&txtDate=%s&random=0.07375017278739282" % dataDate

    #把下载地址发送给requests模块
    f = requests.get(fileAddress1 + fileAddress2)
 
    return f


#dateList = pd.read_csv("D:/Projects/shszexchange_hkholding/shszexchange_hkholding/tradingDateList.csv")
dateList = sql.read_sql("""SELECT DISTINCT DATADATE FROM UQERDATA.TRADING_DAILY WHERE DATADATE>(SELECT MAX(DATADATE) 
                           FROM HKHOLDING.SZ_DAILY_DATA);""", db)["DATADATE"]


for i, tradingDate in enumerate(dateList):
    
    print (i, tradingDate)
    
    outputPath = "D:/Projects/shszexchange_hkholding/shszexchange_hkholding/SZDailyDataFiles/" + "深股通持股数量_%s.xlsx" % tradingDate 

    with open(outputPath,"wb") as dailyData:
        
        dailyData.write(szDataDownload(tradingDate).content)
        
    try:
        dailyData = pd.read_excel(outputPath, dtype={"证券代码" : "str", "证券简称" : "str", "持股数量" : "int"}, thousands=",")
        dailyData["dataDate"] =  tradingDate 
        dailyData = dailyData[["dataDate","证券代码","持股数量"]]
        
        sqlCodeDelete = "DELETE FROM SZ_DAILY_DATA WHERE DATADATE='%s';" % tradingDate
        sqlCodeInsert = "INSERT INTO SZ_DAILY_DATA VALUES (" + ",".join(["%s"] * dailyData.shape[1]) + ");"

        try:
            cursor.execute(sqlCodeDelete)
            db.commit()
            cursor.executemany(sqlCodeInsert, dailyData.values.tolist())
            db.commit()
            print ("Finish updating szse data: %s rows." % str(len(dailyData)))
        except:
            db.rollback()
            print ("Failed")
    except:
        print ("There is no data for date:%s"  % tradingDate)
        
db.close()

#data = pd.read_sql("select * from SZ_DAILY_DATA ORDER BY DATADATE,SECUCODE;", db)


    
