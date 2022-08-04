from http.server import HTTPServer, BaseHTTPRequestHandler
from sys import argv
import json
from kafka import KafkaProducer
from kafka import KafkaAdminClient
BIND_HOST = '0.0.0.0'
PORT = 65432
kafkaProducer = KafkaProducer(bootstrap_servers = ['localhost:9092'],value_serializer = lambda x: json.dumps(x).encode('utf-8'))
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):        
    def do_GET(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length'))
        body = self.rfile.read(content_length)
        data = json.loads(body)
        topic = 'hash'+data['hashtag'][1:]
        kafkaProducer.send(topic, value=data)
        self.send_response(200)
        self.end_headers()


if len(argv) > 1:
    arg = argv[1].split(':')
    BIND_HOST = arg[0]
    PORT = int(arg[1])

print(f'Listening on http://{BIND_HOST}:{PORT}\n')

httpd = HTTPServer((BIND_HOST, PORT), SimpleHTTPRequestHandler)
httpd.serve_forever()