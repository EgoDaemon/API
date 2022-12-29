#!/usr/bin/env python
# coding: utf-8

# In[63]:


import requests
import pandas as pd
import datetime
import time
import re
import sys

import settings

from clickhouse_driver import Client
from clickhouse_key import ch_key


# In[64]:


client = Client(ch_key['db_host'],
               user=ch_key['db_user'],
               password=ch_key['db_password'],
               verify=False,
               database=ch_key['db_name'], settings={'use_numpy': True})


# In[65]:


# pd.options.display.max_columns = None


# In[66]:


def df(result, columns):
    col = []
    for i in columns:
        col.append(i[0])
        df = pd.DataFrame(result,columns=col)
    return df    


# In[67]:


sec_date1 = new_date1 = mob_date1 = jor_date1 = b2b_date1 = (datetime.datetime.today().date() - datetime.timedelta(days=21))
date_to = datetime.datetime.today().date() - datetime.timedelta(days=1)


# In[69]:


keys = [settings.API_KEY_NEW, settings.API_KEY_SEC, settings.API_KEY_B2B, settings.API_KEY_MOB, settings.API_KEY_JOR]
values = [sec_date1, new_date1, b2b_date1, mob_date1, jor_date1]
tokens_dates = {k:v for k, v in zip(keys, values)}


# In[70]:


def money(date_to, tokens_dates):
    try:
        data_you_need=pd.DataFrame()
        for i, y in tokens_dates.items():
            # это нужно для определения в дальнейшем типа продукта (Новостройки, Вторичка, Мобилка.....)
            headers = {"Authorization": "Bearer " + i}
            params = {
                "date_from": y,
                "date_to": date_to,
                "metrics": 'base'
            }
            url4 = "https://target.my.com/api/v3/user.json"
            res4 = requests.get(url4, params=params, headers=headers)
            load4 = res4.json()
            user = load4['username']

            url1 = "https://target.my.com/api/v2/statistics/banners/day.json" 
            money = requests.get(url1, params=params, headers=headers)
            load = money.json()
            time.sleep(1.5)
            in_total = []
            alls = load["items"]
            for i in alls:
                ids = i["id"]
                date = i["rows"]
                for x in date:
                    x.update({
                        "id": ids        
                    })
                    rows = pd.json_normalize(x, sep=" ").to_dict("records")
                    in_total += rows
            df = pd.DataFrame(in_total)
            df['base spent'] = df['base spent'].astype(float)
            rep_filt = df[(df['base spent'] > 0.0)]
            rep_filt = rep_filt.reset_index(drop=True)
            rep_filt['adAccount'] = user
            data_you_need=data_you_need.append(rep_filt,ignore_index=True)
    except (KeyError, AttributeError) as e:
                pass
    return data_you_need


# In[71]:


# кол-по показов в кабинете и в КХ могут не сходится и это ок. некоторые РК сделали показ, но расходов по ним не было.
money = money(date_to, tokens_dates)


# In[95]:


money.head(2)


# In[96]:


def id_list(money_id):
    lst = []
    if money_id.empty == False:
        for i in money_id['id']:
            lst.append(i)
        a = lst
        return a
    else:
#         raise SystemExit("no data for the specified period")
        sys.exit("no data for the specified period")

temp = id_list(money)


# In[106]:


id_list = [] # удаляем дубликаты
[id_list.append(x) for x in temp if x not in id_list]
id_list = id_list


# In[75]:


def find_pattern(string, pattern):
    if re.search(pattern, string):
        return re.search(pattern, string).group(1)

def names(date_to, tokens_dates):
    try:
        data_you_need=pd.DataFrame()
        for i, y in tokens_dates.items():        
            headers = {"Authorization": "Bearer " + i}
            params = {
                "date_from": y,
                "date_to": date_to,
                "metrics": 'base'
            }
            url3 = "https://target.my.com/api/v2/campaigns.json?fields=id,name,utm&limit=250" # увеличила лимит с 200 до 250
            res3 = requests.get(url3, params=params, headers=headers)
            load3 = res3.json()
            tot = load3['items']
            b = pd.DataFrame(tot)
            b = b.rename(columns={"id": "campaign_id"})
            data_you_need=data_you_need.append(b,ignore_index=True)
            pettern_source = '.*utm_source=(.*?)&.*'
            pattern_medium = '.*utm_medium=(.*?)&.*'
            pattern_campaign = '.*utm_campaign=(.*?)&.*'
            pattern_content = '.*utm_content=(.*?)&.*'
            pattern_term = '.*utm_term=(.*?)$'
            data_you_need['utm'] = data_you_need['utm'].astype(str)
            data_you_need['utm_source'] = data_you_need['utm'].apply(lambda x: find_pattern(x,pettern_source)).astype(str)
            data_you_need['utm_medium'] = data_you_need['utm'].apply(lambda x: find_pattern(x,pattern_medium)).astype(str)
            data_you_need['utm_campaign'] = data_you_need['utm'].apply(lambda x: find_pattern(x,pattern_campaign)).astype(str)
            data_you_need['utm_content'] = data_you_need['utm'].apply(lambda x: find_pattern(x,pattern_content)).astype(str)
            data_you_need['utm_term'] = data_you_need['utm'].apply(lambda x: find_pattern(x,pattern_term)).astype(str)
    except (KeyError, AttributeError) as e:
                pass
    return data_you_need

names = names(date_to, tokens_dates)


# In[99]:


def urls(id_list, date_to, tokens_dates):
    data_you_need=pd.DataFrame()
    for x, y in tokens_dates.items(): 
        utms_upd = []
        for i in id_list:        
            headers = {"Authorization": "Bearer " + x}            
            params = {
                "date_from": y,
                "date_to": date_to,
                "metrics": 'base'
                }
            url2 = f"https://target.my.com/api/v2/banners/{i}.json?fields=id,campaign_id,urls&limit=200"
            res2 = requests.get(url2, params=params, headers=headers)
            time.sleep(1.0)
            load2 = res2.json()
            utms_upd.append(load2)        
            a = pd.DataFrame(utms_upd)
        data_you_need=data_you_need.append(a,ignore_index=True)            
    return data_you_need

urls = urls(id_list, date_to, tokens_dates)


# In[101]:


urls = urls.drop("error", axis=1).dropna().reset_index(drop=True).copy()
urls['campaign_id'] = urls['campaign_id'].astype(int)
urls['id'] = urls['id'].astype(int)
urls['urls'] = urls['urls'].astype(str)
urls = urls.drop_duplicates()


# In[65]:


def url_sep2(urls):
    try:
        pattern_full_url = '{\'primary\'.*\'url\': \'(.*?)\',.*'
        pettern_source = '.*utm_source=(.*?)&.*'
        pattern_medium = '.*utm_medium=(.*?)&.*'
        pattern_campaign = '.*utm_campaign=(.*?)&.*'
        pattern_content = '.*utm_content=(.*?)&.*'
        pattern_term = '.*utm_term=(.*?)$'
        urls['urls'] = urls['urls'].astype(str)
        urls['urls'] = urls['urls'].apply(lambda x: find_pattern(x,pattern_full_url)).astype(str)
        urls['utm_s'] = urls['urls'].apply(lambda x: find_pattern(x,pettern_source)).astype(str)
        urls['utm_m'] = urls['urls'].apply(lambda x: find_pattern(x,pattern_medium)).astype(str)
        urls['utm_cam'] = urls['urls'].apply(lambda x: find_pattern(x,pattern_campaign)).astype(str)
        urls['utm_con'] = urls['urls'].apply(lambda x: find_pattern(x,pattern_content)).astype(str)
        urls['utm_t'] = urls['urls'].apply(lambda x: find_pattern(x,pattern_term)).astype(str)
        return urls
    except (KeyError, AttributeError) as e:
        print('end')

clear = url_sep2(urls)


# In[66]:


clear = clear.drop_duplicates()


# In[67]:


start = names.merge(clear, how = 'inner', on = 'campaign_id')


# In[68]:


start['urls'] = start['urls'].astype(str)
start['utm_source'] = start['utm_source'].astype(str)


# In[69]:


# Замена значения одних колонок на другие. Важна последовательность!!! Не менять
# s.loc[s['n_reviews'] > 120, ['success']] = s['price']

idx = (start.query("utm_source == 'None'").index.to_series().to_numpy())
start.loc[idx, "utm_source"] = start.loc[idx, "utm_s"]
idx = (start.query("utm_medium == 'None'").index.to_series().to_numpy())
start.loc[idx, "utm_medium"] = start.loc[idx, "utm_m"]
idx = (start.query("utm_campaign == 'None'").index.to_series().to_numpy())
start.loc[idx, "utm_campaign"] = start.loc[idx, "utm_cam"]
idx = (start.query("utm_content == 'None'").index.to_series().to_numpy())
start.loc[idx, "utm_content"] = start.loc[idx, "utm_con"]
idx = (start.query("utm_term == 'None'").index.to_series().to_numpy())
start.loc[idx, "utm_term"] = start.loc[idx, "utm_t"]

idx = (start.query("utm_content == '{{campaign_id}}'").index.to_series().to_numpy())
start.loc[idx, "utm_content"] = start.loc[idx, "campaign_id"]
idx = (start.query("utm_term == '{{banner_id}}'").index.to_series().to_numpy())
start.loc[idx, "utm_term"] = start.loc[idx, "id"]

idx = (start.query("utm_content == 'None'").index.to_series().to_numpy())
start.loc[idx, "utm_content"] = start.loc[idx, "campaign_id"]
idx = (start.query("utm_term == 'None'").index.to_series().to_numpy())
start.loc[idx, "utm_term"] = start.loc[idx, "id"]


# In[71]:


start['utm_campaign'] = start.utm_campaign.apply(lambda x: 'no campaign name' if x == 'None' else x)
start['utm_source'] = start.utm_source.apply(lambda x: 'target.my.com' if x != 'mytarget' else x)
start['utm_medium'] = start.utm_medium.apply(lambda x: 'cpc' if x == 'cpc' else ('cpm' if x == 'cpm'  else ('cpi' if x == 'cpi'  else 'referral')))

start = start.drop_duplicates()


# In[72]:


def types(df):
    if '9644e23da9@agency_client' in df:
        return 'Классифайд'
    elif '8ee847846c@agency_client' in df:
        return 'Новостройки'
    elif 'ef427e7659@agency_client' in df:
        return 'Mobile'
    elif 'fa64982560@agency_client' in df:
        return 'Журнал'
    elif '8b193322ff@agency_client' in df:
        return 'B2B'
    else:
        return 'no type'
    raise Exception("какая-то хрень")

money['type'] = money['adAccount'].apply(types)


# In[74]:


finish = money.merge(start, how='inner', on = 'id')
finish['date'] = pd.to_datetime(finish['date'], format='%Y-%m-%d')


# In[75]:


finish = finish.rename(columns = {
 'id':'banner_id',
 'urls':'url',
 'name':'CampaignName',
 'base shows':'Impressions',
 'base clicks': 'Clicks',
 'base spent': 'Cost'
}).fillna(0)
for_load = finish[['type','date','CampaignName','campaign_id', 'banner_id','url','utm_source','utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'Impressions', 'Clicks', 'Cost', 'adAccount']].copy()


# In[76]:


for_load['utm_content'] = for_load['utm_content'].astype(str)
for_load['utm_term'] = for_load['utm_term'].astype(str)


# In[77]:


# client.execute('''CREATE TABLE external.MT_ADS(type String, date Date, CampaignName String, campaign_id Int64, 
# banner_id  Int64, url String, utm_source String, utm_medium String, utm_campaign String, utm_content String, utm_term String, Impressions Int64, Clicks Int64, Cost Float64, adAccount String) ENGINE = MergeTree() ORDER BY (date, type)'''
# )


# In[16]:


client.execute(f''' ALTER TABLE external.MT_ADS DELETE WHERE date between '{sec_date1}' AND '{date_to}' ''')


# In[78]:


client.insert_dataframe("INSERT INTO external.MT_ADS VALUES", for_load) #VALUES - DO NOT FORGET TO SET

