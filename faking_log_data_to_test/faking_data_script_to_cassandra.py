from cassandra.cluster import Cluster
from datetime import datetime, timedelta
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

keyspace = 'study_data_engineering'
cassandra_login = 'cassandra'
cassandra_password = 'cassandra'
cluster = Cluster()
session = cluster.connect(keyspace)

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


def generating_dummy_data(n_records,session):
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
        sql = """ INSERT INTO tracking (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) VALUES ('{}',{},{},'{}',{},{},{},'{}')""".format(create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts)
        print(sql)
        session.execute(sql)
        i+=1 
    return print("Data Generated Successfully")

status = "ON"
while status == "ON":
    generating_dummy_data(n_records = random.randint(1,20),session = session)
    time.sleep(10)


