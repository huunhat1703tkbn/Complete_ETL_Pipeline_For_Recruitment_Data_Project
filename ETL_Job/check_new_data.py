from pyspark.sql import SparkSession
from login_mysql import USER, PASSWORD, URL, DRIVER

def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'study_data_engineering').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time():    
    sql = """(select max(latest_update_time) from events) data"""
    mysql_time = spark.read.format('jdbc').options(url=URL, driver=DRIVER, dbtable=sql, user=USER, password=PASSWORD).load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '2003-03-17 00:00:00'
    else :
        mysql_latest = mysql_time.strftime("%Y-%m-%d %H:%M:%S")
    return mysql_latest

def main():
    cassandra_time = get_latest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = get_mysql_latest_time()
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time : 
        print('Have new data, continue task')
        spark.stop()
        return True
    else :
        spark.stop()
        return False

if __name__ == "__main__":
    spark = SparkSession.builder \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
            .getOrCreate()
    main()
    