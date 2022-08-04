import socket
import kafka
from kafka import KafkaProducer
import pandas as pd
import json
import time
df = pd.read_csv('Hello.csv')
admin_client = kafka.KafkaAdminClient(bootstrap_servers=['localhost:9092'])
topics_current = admin_client.list_topics()
my_producer = KafkaProducer(  
     bootstrap_servers = ['localhost:9092'],  
     value_serializer = lambda x: json.dumps(x).encode('utf-8'))
for i in range(len(df)):
     #my_producer.send('sample', key = df.loc[i, 'key'], value = str(df.loc[i, 'value']))
     topic = 'hash'+df.loc[i,'key'][1:]
     if topic not in topics_current:
          admin_client.create_topics([kafka.admin.NewTopic(name=topic, num_partitions=1, replication_factor=1)])
          topics_current.append(topic)
          time.sleep(1)
     my_producer.send(topic, value = {'hashtag':str(df.loc[i, 'key']), 'count':int(df.loc[i,'value']), 'timestamp':str(df.loc[i,'timestamp'])})
     my_producer.flush()