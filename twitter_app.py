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


# def get_tweets():
# 	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
# 	query_data = [('track','#')]
# 	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
# 	response = requests.get(query_url, auth=my_auth, stream=True)
# 	print(query_url, response)
# 	return response


# def send_tweets_to_spark(http_resp, tcp_connection):
#     for line in http_resp.iter_lines():
#         try:
#         	full_tweet = json.loads(line)
#         	tweet_text = full_tweet['text']
#         	print("Tweet Text: " + tweet_text)
#         	print ("------------------------------------------")
#         	tcp_connection.send(tweet_text + '\n')
#     	except:
# 		pass
#  #       	e = sys.exc_info()[0]
#  #      	print("Error: %s" % e)



# def main():
#     TCP_IP = "localhost"
#     TCP_PORT = 9009
#     conn = None
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.bind((TCP_IP, TCP_PORT))
#     s.listen(1)
#     print("Waiting for TCP connection...")
#     conn, addr = s.accept()
#     print("Connected... Starting getting tweets.")
#     resp = get_tweets()
#     send_tweets_to_spark(resp, conn)

if __name__ == '__main__':
#    my_auth = requests_oauthlib.OAuth1(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
     #my_auth = requests_oauthlib.OAuth1( CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)
     main()    
