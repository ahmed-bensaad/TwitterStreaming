from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import socket
import json
from credentials import *



subject = "football"

class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket
 
    def on_data(self, data):
        try:
            print(json.loads(data)["text"])
            self.client_socket.send(data.encode())
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True
    
def sendData(c_socket):
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
 
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=[subject])

def main():
        s = socket.socket()     # Create a socket object
        host = "localhost"      # Get local machine name
        port = 6006             # Reserve a port for your service.
        s.bind((host, port))    # Bind to the port
 
        print("Listening on port: %s" % str(port))
 
        s.listen(5)                 # Now wait for client connection.
        c, addr = s.accept()        # Establish connection with client.
 
        print( "Received request from: " + str( addr ) )
 
        sendData( c )



if __name__ == '__main__':
     main()    
