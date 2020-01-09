import flask
from google.cloud import bigquery
from google.oauth2 import service_account
from flask import jsonify
from flask_cors import CORS
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Wifi Analytics -a9e4f34ee68d.json"
client = bigquery.Client()
bigquery_client = bigquery.Client()
dataset_ref = bigquery_client.dataset('wifi-analytics-251106')
#################################

query = """SELECT * from `wifi-analytics-251106.RTLS_Feed.Triangulation` """
query_job = bigquery_client.query(query)
data = query_job.result()
heat = data.to_dataframe()
heat = heat.values.tolist()




####################################
query = """select Date,count(client_mac) as Usercount from 
(SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ) as  date, client_mac,TIMESTAMP_DIFF(Max(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)), Min(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)), minute) as duration FROM `wifi-analytics-251106.RTLS_Feed.RTLS_Data` where ap_mac in ('20a6cdcc0616','20a6cdcc0546','20a6cdcbf02c','20a6cdcc07f6','20a6cdcc103e','20a6cdcbec4a','20a6cdcbe9f2','20a6cdcbea60','20a6cdcbc58a','20a6cdcbeb7a','20a6cdcbea88','20a6cdcbebba','20a6cdcbee44','20a6cdcbebdc','20a6cdcc0532') group by date, client_mac order by date) A where Duration < 300 group by  date"""

query_job = bigquery_client.query(query)
data = query_job.result()
visits = data.to_dataframe()
visits = visits.values.tolist()
print(visits)
#################################
query = """select date , round((count(case when A.Duration >= 30 and A.Duration < 300 then 1  end)/count(case when A.Duration < 300 then client_mac end))*100,2) as engaged_visits,round((count(case when A.Duration < 30  then 1  end)/count( case when A.Duration < 300 then client_mac end ))*100,2) as Bounced_visits from
(SELECT distinct client_mac , Min( TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ) as Enterytime,Max( TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ) as Exitime,TIMESTAMP_DIFF(Max( TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ), Min( TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ), MINUTE) as Duration,EXTRACT(DATE FROM TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ) as date FROM `wifi-analytics-251106.RTLS_Feed.RTLS_Data` where ap_mac in ('20a6cdcc0616','20a6cdcc0546','20a6cdcbf02c','20a6cdcc07f6','20a6cdcc103e','20a6cdcbec4a','20a6cdcbe9f2','20a6cdcbea60','20a6cdcbc58a','20a6cdcbeb7a','20a6cdcbea88','20a6cdcbebba','20a6cdcbee44','20a6cdcbebdc','20a6cdcc0532')  
group by client_mac ,date ) A 
group by date
order by DATE"""
query_job = bigquery_client.query(query)
data = query_job.result()
bou_eng = data.to_dataframe()
bou_eng = bou_eng.values.tolist()
#################################
query = """select date,ROUND(AVG(duration),2) as Dwelltime_minutes from
(SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ) as  date, client_mac,Min(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)) as Enterytime,Max(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)) as Exitime,TIMESTAMP_DIFF(Max(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)), Min(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)), minute) as duration FROM `wifi-analytics-251106.RTLS_Feed.RTLS_Data` where ap_mac in ('20a6cdcc0616','20a6cdcc0546','20a6cdcbf02c','20a6cdcc07f6','20a6cdcc103e','20a6cdcbec4a','20a6cdcbe9f2','20a6cdcbea60','20a6cdcbc58a','20a6cdcbeb7a','20a6cdcbea88','20a6cdcbebba','20a6cdcbee44','20a6cdcbebdc','20a6cdcc0532') group by date, client_mac order by date) A where Duration < 300
group by date order by date"""
query_job = bigquery_client.query(query)
data = query_job.result()
dwelltime = data.to_dataframe()
dwelltime = dwelltime.values.tolist()
#################################
query = """select count(case when count > 1 then 1 end)/count(count) as loyal_customer_per from 
(select client_mac,count(client_mac) as count from
(select date,client_mac,count( client_mac ) as usercount from
(SELECT  distinct client_mac,EXTRACT(DATE FROM TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ) as  date,count(client_mac) as usercount,TIMESTAMP_DIFF(Max(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)) , Min(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)), minute) as duration FROM `wifi-analytics-251106.RTLS_Feed.RTLS_Data` where ap_mac in ('20a6cdcc0616','20a6cdcc0546','20a6cdcbf02c','20a6cdcc07f6','20a6cdcc103e','20a6cdcbec4a','20a6cdcbe9f2','20a6cdcbea60','20a6cdcbc58a','20a6cdcbeb7a','20a6cdcbea88','20a6cdcbebba','20a6cdcbee44','20a6cdcbebdc','20a6cdcc0532')  group by date, client_mac having Duration < 300
order by date,client_mac) A
group by date,client_mac
order by date) B
group by client_mac order by count desc) C"""
query_job = bigquery_client.query(query)
data = query_job.result()
loyality = data.to_dataframe()
loyality = loyality.values.tolist()
#################################
query = """select sum(count) as visits,bucket from
(select count,case when count =1 then 1
when count  between 2 and 4  then 2
when count between 4 and 6 then 6
else 7 end as bucket
from 
(select client_mac,count(client_mac) as count from
(SELECT   client_mac ,EXTRACT(DATE FROM TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)) as  date,count(client_mac)  as usercount,TIMESTAMP_DIFF(Max(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)) , Min(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)), minute) as duration FROM `wifi-analytics-251106.RTLS_Feed.RTLS_Data` where ap_mac in ('20a6cdcc0616','20a6cdcc0546','20a6cdcbf02c','20a6cdcc07f6','20a6cdcc103e','20a6cdcbec4a','20a6cdcbe9f2','20a6cdcbea60','20a6cdcbc58a','20a6cdcbeb7a','20a6cdcbea88','20a6cdcbebba','20a6cdcbee44','20a6cdcbebdc','20a6cdcc0532')  group by date, client_mac having duration < 300
order by date,client_mac) A group by client_mac order by count desc) C) group by  bucket
"""
query_job = bigquery_client.query(query)
data = query_job.result()
multiple_visits = data.to_dataframe()
multiple_visits = multiple_visits.values.tolist()
#################################
query = """select date,MacAddress,Enterytime,Exitime,duration from
(SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR) ) as  date, client_mac as MacAddress,Min(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)) as Enterytime,Max(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)) as Exitime,TIMESTAMP_DIFF(Max(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)), Min(TIMESTAMP_ADD(timestamp, INTERVAL 04 HOUR)), minute) as duration FROM `wifi-analytics-251106.RTLS_Feed.RTLS_Data` where ap_mac in ('20a6cdcc0616','20a6cdcc0546','20a6cdcbf02c','20a6cdcc07f6','20a6cdcc103e','20a6cdcbec4a','20a6cdcbe9f2','20a6cdcbea60','20a6cdcbc58a','20a6cdcbeb7a','20a6cdcbea88','20a6cdcbebba','20a6cdcbee44','20a6cdcbebdc','20a6cdcc0532') group by date, client_mac order by date) A where Duration < 300
"""
query_job = bigquery_client.query(query)
data = query_job.result()
dwell_time_breakdown = data.to_dataframe()
dwell_time_breakdown = dwell_time_breakdown.values.tolist()
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

@app.route('/heat', methods=['GET'])
def heats():
    return jsonify(heat)

app.run()




