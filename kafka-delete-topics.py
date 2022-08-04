import kafka
admin_client = kafka.KafkaAdminClient(bootstrap_servers=['localhost:9092'])
admin_client.delete_topics(admin_client.list_topics())