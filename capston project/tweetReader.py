import tweepy
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import socket
import json
from .credentials import *

class TweerListener(StreamListener):

    def __int__(self, c_socket):
        self.client_socket = c_socket

    def on_data(self, data):

        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True

        except BaseException as e:
            print("ERROR ",e)
            return True

        def on_error(self, status):
            print(status)
            return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweerListener)
    twitter_stream.filter(track=['guitar'])

if __name__== '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 9998
    s.bind((host, port))

    print("Listening on port 5555")

    s.listen(5)
    c, addr = s.accept()

    sendData(c)