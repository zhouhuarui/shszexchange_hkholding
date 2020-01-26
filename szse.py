"""
本脚本用于从深交所直接下载深股通每日日终持仓数据
交易所直接给XLSX格式文件，若有数据则基本保证是完整数据文件，若未公布或该日无数据则依然可下载文件但文件内容为“没有找到符合条件的数据！”
本脚本只完成下载功能，无数据校验
"""
import requests

def szDataDownload(dataDate):

    # dataDate = "2020-01-26"
    outputPath = "D:/工作/shszexchange_hkholding/shszexchange_hkholding/SZDailyDataFiles/"

    #下载地址
    fileAddress1 = "http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx"
    fileAddress2 = "&CATALOGID=SGT_SGTCGSL&TABKEY=tab1&txtDate=%s&random=0.07375017278739282" % dataDate

    #把下载地址发送给requests模块
    f = requests.get(fileAddress1 + fileAddress2)

    #下载文件
    with open(outputPath + "深股通持股数量_%s.xlsx" % dataDate,"wb") as dailyData:
        
        dailyData.write(f.content)

