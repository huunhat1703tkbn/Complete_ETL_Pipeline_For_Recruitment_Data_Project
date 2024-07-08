from datetime import datetime, timedelta
from kafka import KafkaProducer
import time
import cassandra
import random
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
import datetime
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
from login_mysql import USER, PASSWORD, HOST, PORT, DB_NAME, URL, DRIVER
import json

kafka_bootstrap_severs = "192.168.56.1:9092"
producer = KafkaProducer(bootstrap_servers= kafka_bootstrap_severs, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
kafka_topic = "test1"

def get_data_from_job():
    cnx = mysql.connector.connect(user=USER, password=PASSWORD,
                                         host=HOST,
                                      database=DB_NAME)
    query = """select id as job_id,campaign_id , group_id , company_id from job"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def get_data_from_publisher():
    cnx = mysql.connector.connect(user=USER, password=PASSWORD,
                                         host=HOST,
                                      database=DB_NAME)
    query = """select distinct(id) as publisher_id from master_publisher"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data


def generating_dummy_data(n_records):
    publisher = get_data_from_publisher()
    publisher = publisher['publisher_id'].to_list()
    jobs_data = get_data_from_job()
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).to_list()
    i = 0 
    fake_records = n_records
    while i <= fake_records:
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.now()))
        bid = random.randint(0,1)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact,weights=(70,10,10,10))[0]
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "create_time": create_time,
            "bid":bid,
            "campaign_id":campaign_id,
            "custom_track":custom_track,
            "group_id":group_id,
            "job_id":job_id,
            "publisher_id":publisher_id,
            "ts":ts,
        }
        print(data)
        producer.send(kafka_topic, value=data)
        i+=1 
    return print("Data Generated Successfully")

status = "ON"
while status == "ON":
    generating_dummy_data(n_records = random.randint(1,5))
    time.sleep(1)
producer.close()

