# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 20:35:29 2020

@author: zhouh
"""
import numpy as np
import pandas as pd
import pymysql
db = pymysql.connect("localhost", "root", "myangelxjl","hkholding" )
cursor = db.cursor()
import time
from WindPy import w
w.start()

dateList = pd.read_csv("D:/Projects/shszexchange_hkholding/shszexchange_hkholding/dateList.csv")
dateList = dateList["tradingDate"]

for updateDate in  dateList:
    
#    updateDate = str(pd.to_datetime(updateDate))[:10]
    print (updateDate)
    
    
    #沪股通
    dailyDataSH = w.wset("shstockholdings","date=%s" % updateDate,
                         "field=wind_code,hold_stocks,publish_ratio,calculate_ratio,float_sharesratio")
    dailyDataSH = pd.DataFrame(np.array(dailyDataSH.Data).T, 
                               columns=["wind_code","hold_stocks","publish_ratio","calculate_ratio","float_sharesratio"])
    dailyDataSH = dailyDataSH.astype({"wind_code" : str, "hold_stocks" : float, "publish_ratio" : float, 
                                      "calculate_ratio" : float, "float_sharesratio" : float})
    dailyDataSH["wind_code"] = dailyDataSH["wind_code"].str.slice(0, 6)
    
    
    #深股通
    dailyDataSZ = w.wset("szstockholdings","date=%s" % updateDate,
                         "field=wind_code,hold_stocks,publish_ratio,calculate_ratio,float_sharesratio")
    dailyDataSZ = pd.DataFrame(np.array(dailyDataSZ.Data).T, 
                               columns=["wind_code","hold_stocks","publish_ratio","calculate_ratio","float_sharesratio"])
    dailyDataSZ = dailyDataSZ.astype({"wind_code" : str, "hold_stocks" : float, "publish_ratio" : float, 
                                      "calculate_ratio" : float, "float_sharesratio" : float})
    dailyDataSZ["wind_code"] = dailyDataSZ["wind_code"].str.slice(0, 6)
    
    dailyData = dailyDataSH.append(dailyDataSZ)
    dailyData["dataDate"] = updateDate
    
    
    sqlCodeDelete = "DELETE FROM WIND_DATA_DAILY WHERE DATADATE='%s';" % updateDate
    sqlCodeInsert = "INSERT INTO WIND_DATA_DAILY VALUES (" + ",".join(["%s"] * dailyData.shape[1]) + ");"
    
    
    try:
        dailyData = dailyData[["dataDate","wind_code","hold_stocks","publish_ratio","calculate_ratio","float_sharesratio"]]
        #与SQL的空值兼容问题
        dailyData = dailyData.where(dailyData.notnull(), None)   
    
        try:
            cursor.execute(sqlCodeDelete)
            db.commit()
            cursor.executemany(sqlCodeInsert, dailyData.values.tolist())
            db.commit()
            print ("Finish updating daily trading data: %s rows." % str(len(dailyData)))
        except:
            db.rollback()
            print ("Failed")
    except:
        print ("There is no data for date:%s"  % updateDate)
    
    time.sleep(10)
    
db.close()
















