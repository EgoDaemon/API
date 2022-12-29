from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from numpy import dtype
from oauth2client.service_account import ServiceAccountCredentials
import csv
import datetime
import json
import numpy
import pandas
import requests
import string
import sys
import urllib
import os
import pandas
import time
import datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    MetricType,
    RunReportRequest,
    Filter,
    FilterExpression,
    FilterExpressionList
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "rising-minutia-372107-3f00351690a6.json"
key_path =  "rising-minutia-372107-3f00351690a6.json"
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
class ga4:
    #   Задаём ключ из файла
        
    
    def __init__(self, propertie = '344897288'):
    # Оптределяем аккаунт откуда бер1м данные
        self.propertie = propertie
        self.client = BetaAnalyticsDataClient()

    def parse_response(self, rows_obj):
        vals = []
        cols = [i.name for i in rows_obj.dimension_headers] + [i.name for i in rows_obj.metric_headers]
    #     print('Processing IN: ',len(rows_obj.rows))
        for i in range(0, len(rows_obj.rows)):
            row = rows_obj.rows[i]
            val = [i.value for i in row.dimension_values]+[i.value for i in row.metric_values]
            vals.append(val)
        res = pandas.DataFrame(vals, columns = cols)
    #     print('Processing IN: ',len(res))
        return res
    
    def ga4_report(self, dimetions, metrics, dates , filters = {}, offset = 0):
        
        print(dimetions, metrics, dates , filters, offset)
        dims = [Dimension(name=i) for i in dimetions]
        metrics = [Metric(name=i) for i in metrics]
        
        
        if filters == {}:
            request = RunReportRequest(
            property=f"properties/{self.propertie}",
            dimensions=dims,
            metrics=metrics,
            offset = offset,
            date_ranges= [DateRange(start_date=dates[0], end_date=dates[1])],
            )
        else:
            filters = [FilterExpression(filter=Filter(field_name=i,
            string_filter=Filter.StringFilter(value=e),)) for i,e in filters.items()]
            request = RunReportRequest(
            property=f"properties/{self.propertie}",
            dimensions=dims,
            metrics=metrics,
            offset = offset,
            dimension_filter = FilterExpression(and_group=FilterExpressionList(expressions=filters)),
            date_ranges= [DateRange(start_date=dates[0], end_date=dates[1])],)
        
        return self.client.run_report(request)


    def report_get(self, dimetions, metrics, dates, filters = {}):
#     Отдаём таблицу готовых данных
        report = self.ga4_report(dimetions, metrics, dates, filters)
        columns = [i.name for i in report.dimension_headers] + [i.name for i in report.metric_headers]
        
        full_rows = report.row_count
        print('Full report len: ', full_rows)
        if full_rows == 0:
            return ([], columns)
        report_lenth= full_rows
        
        res = self.parse_response(report)
        page = 0
        while report_lenth > 10000:
            page +=10000
            
            
            report_extra = self.ga4_report(dates= dates, metrics = metrics, dimetions = dimetions, filters = filters,
                                          offset = page)
            print('Page: ', page)
            print('Rows: ', len(report_extra.rows))
            res1 = self.parse_response(report_extra)
            res = pandas.concat([res,res1])
            res = res.reset_index(drop=True)
            print('Pandas: ', len(res))
            report_lenth -= 10000
            time.sleep(3)       
        return res
    
def clean_pd_page(pd, date):
    
    pd['pagePath1'] = pd['pagePath'].apply(lambda x: x.split('/')[1])
    pd['date'] = date
    pd['pagePath2'] = pd['pagePath'].apply(lambda x: '' if len(x.split('/')) < 3  else x.split('/')[2])
    pd['pagePath3'] = pd['pagePath'].apply(lambda x: '' if len(x.split('/')) < 4  else x.split('/')[3])
    pd['pagePath_type'] = pd['pageReferrer'].apply(lambda x: 'Internal' if 'https://rehold.com' in x else 'External')
    return pd