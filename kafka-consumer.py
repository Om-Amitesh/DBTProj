from ensurepip import bootstrap
from kafka import KafkaConsumer
import json
import psycopg2 as ps
con = ps.connect(user='postgres', host='127.0.0.1', port='5432', database = 'Tweets')
cur = con.cursor()
consumer = KafkaConsumer(
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     metadata_max_age_ms = 1,
     enable_auto_commit=True,
     value_deserializer = lambda m: json.loads(m.decode()))
consumer.subscribe(pattern='hash.*')
for message in consumer:
    data = message.value
    print(data['hashtag'], data['count'], data['timestamp'])
    cur.execute(f'''insert into counts values(\'{data["hashtag"]}\', {data['count']}, \'{data['timestamp']}\')''')
    con.commit()