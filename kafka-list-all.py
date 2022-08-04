import kafka
admin_client = kafka.KafkaAdminClient(bootstrap_servers=['localhost:9092'])
print(admin_client.list_topics())