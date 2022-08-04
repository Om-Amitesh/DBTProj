import twitter_creds
import tweepy
import json
import socket
Msocket = socket.socket()
host = '0.0.0.0'
port = 3000
Msocket.bind((host, port))
Msocket.listen(4)
c_socket, addr = Msocket.accept()
class CustomStream(tweepy.Stream):
    def on_data(self, data):
        try:
            tweet = json.loads(data)
            c_socket.send(str(tweet['text']).encode())
            print('new tweet')
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True
          

    def on_error(self, status):
        print(status)
# class CustomStreamClient(tweepy.StreamingClient):
#     def on_tweet(self, tweet):
#         try:
#             print(tweet)
#             with open('data.json', 'a') as tf:
#                 tf.write(str(tweet))
#             return True
#         except BaseException as e:
#             print("Error on_data %s" % str(e))
#         return True
if __name__ == '__main__':
    stream = CustomStream(twitter_creds.CONSUMER_KEY,twitter_creds.CONSUMER_KEY_SECRET, twitter_creds.ACCESS_TOKEN, twitter_creds.ACCESS_TOKEN_SECRET)
    stream.filter(track=['#ARSMUN', '#NBA', '#MIvsLSG', '#KGF2', '#WillSmith'])