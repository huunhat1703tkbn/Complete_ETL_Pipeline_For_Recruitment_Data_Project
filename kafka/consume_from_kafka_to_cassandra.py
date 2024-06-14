from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import multiprocessing


def consume_from_kafka(cassandra_keyspace, kafka_topic, kafka_bootstrap_severs ):
    cluster = Cluster()
    session = cluster.connect(cassandra_keyspace)
    consumer = KafkaConsumer(kafka_topic, bootstrap_servers= kafka_bootstrap_severs, value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    for message in consumer:
        data = message.value
        query = """INSERT INTO tracking (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) VALUES ('{}',{},{},'{}',{},{},{},'{}')""".format(data["create_time"], data["bid"], data["campaign_id"], data["custom_track"], data["group_id"], data["job_id"], data["publisher_id"], data["ts"])
        print(query)
        session.execute(query)
        print("Reading data from kafka topic and write to cassandra done !!!")

    consumer.close()
    cluster.shutdown()

if __name__ == "__main__":
    kafka_bootstrap_severs = "192.168.56.1:9092"
    kafka_topic = "myproject"
    cassandra_keyspace = 'study_data_engineering'

    process = multiprocessing.Process(target=consume_from_kafka, args= (cassandra_keyspace, kafka_topic, kafka_bootstrap_severs))
    process.start()
    process.join()