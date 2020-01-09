#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pydocumentdb import document_client
import pandasql as ps
import pandas as pd
import numpy as np
import datetime as dt
from flask import jsonify
from flask_cors import CORS
import os
import flask

uri = 'https://rtlsdata.documents.azure.com:443/'
key = 'xmjbWNlvR4zPEgBlTklvtlevI7ciaAs7Qbes7dNbLjDQze1ci6m1nwMVXSocqkmCEYodrP1qmSt5DtbBm8v8dg=='
client = document_client.DocumentClient(uri, {'masterKey': key})
db_id = 'rtlsdb'
db_query = "select * from rtlstest"
db = list(client.QueryDatabases(db_query))
db_link = db[2]['_self']

coll_id = 'rtlsdb'
coll_query = "select * from rtlstest"
coll = list(client.QueryCollections(db_link, coll_query))[0]
coll_link  = coll['_self']


# In[2]:


docs = client.ReadDocuments(coll_link)
data = list(docs)
df = pd.DataFrame(data)


# In[4]:


df['dates'] = np.nan
df['timestamp'] = df['timestamp'].apply(lambda x : pd.to_datetime(str(x)))
df['dates'] = df['timestamp'].dt.date


# In[18]:


df = df[df.timestamp.notnull()]


# In[139]:


q1 = """select distinct client_mac,dates,min(timestamp) as Enterytime,max(timestamp) as Exittime from df group by dates,client_mac"""
df2 = ps.sqldf(q1, locals())


# In[140]:


df2['Enterytime'] = df2['Enterytime'].apply(lambda x : pd.to_datetime(str(x)))
df2['Exittime'] = df2['Exittime'].apply(lambda x : pd.to_datetime(str(x)))
df2['duration'] = np.nan
for i in range(df2.shape[0]):
    df2['duration'][i] = df2['Exittime'][i] - df2['Enterytime'][i]
for i in range(df2.shape[0]):
    df2['duration'][i] = df2['duration'][i].total_seconds()/60


# In[147]:


q1 = """select dates as Date,count(distinct client_mac) as Usercount from df group by Date"""
visits = ps.sqldf(q1, locals())
visits = visits.values.tolist()


# In[142]:


q1 = """select dates as Date,ROUND((cast(count(case when duration >= 30 and duration<300 then 1  end) as float)/cast(count(case when duration < 300 then client_mac end) as float))*100,2) as Engaged_visits,round((cast(count(case when duration < 30  then 1  end) as float)/cast(count( case when duration < 300 then client_mac end ) as float))*100,2) as Bounced_visits  from df2 group by Date"""
bou_eng = ps.sqldf(q1, locals())
bou_eng = bou_eng.values.tolist()


# In[143]:


q1 = """select dates as date,ROUND(AVG(duration),2) as Dwelltime_minutes from df2 group by date"""
dwelltime = ps.sqldf(q1, locals())
dwelltime = dwelltime.values.tolist()


# In[145]:


q1 = """select * from df2"""
dwell_time_breakdown = ps.sqldf(q1, locals())
dwell_time_breakdown = dwell_time_breakdown.values.tolist()


# In[125]:


q1 = """select cast(count(case when usercount > 1 then 1 end) as float)/cast(count(usercount) as float) as loyal_customer_per from (select client_mac,count(client_mac) as usercount from (select   client_mac,count(client_mac) as usercount,dates from df group by dates,client_mac) group by client_mac)"""
loyality = ps.sqldf(q1, locals())
loyality = loyality.values.tolist()


# In[146]:


q1 = """select sum(count) as visits,bucket from
(select count,case when count =1 then 1
when count  between 2 and 4  then 2
when count between 4 and 6 then 6
else 7 end as bucket
from 
(select client_mac,count(client_mac) as count from
(SELECT   client_mac ,dates,count(client_mac)  as usercount FROM df group by dates, client_mac order by dates,client_mac) A group by client_mac order by count desc) C) group by  bucket"""
multiple_visits = ps.sqldf(q1, locals())
multiple_visits = multiple_visits.values.tolist()


# In[ ]:


#################################
app = flask.Flask(__name__)
app.config["DEBUG"] = True
CORS(app)
@app.route('/visits', methods=['GET'])
def visit():
    return jsonify(visits)

@app.route('/bounced_engaged_visits', methods=['GET'])
def be():
    return jsonify(bou_eng)

@app.route('/dwelltime', methods=['GET'])
def dw():
    return jsonify(dwelltime)

@app.route('/loyality', methods=['GET'])
def lw():
    return jsonify(loyality)

@app.route('/multiple_visits', methods=['GET'])
def mv():
    return jsonify(multiple_visits)

@app.route('/dwell_time_breakdown', methods=['GET'])
def dwb():
    return jsonify(dwell_time_breakdown)

# @app.route('/heat', methods=['GET'])
# def heats():
#     return jsonify(heat)
app.run()


# In[ ]:




