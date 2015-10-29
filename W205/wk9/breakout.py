from __future__ import print_function
import sys
import tweepy
import datetime
import urllib
import signal
import json
import time

class TweetStore:
   fileCount = 0
   pathPattern = None
   file = None

   def __init__(self, pathPattern = "%d/tweets-%n"):
      self.pathPattern = pathPattern

   def newFile():
      self.close()
      fileCount += 1
      pat = pathPattern.replace("%n", fileCount)
      path = time.strftime(pat)
      d = os.path.dirname(path)
      if not os.path.exists(d):
	 os.makedirs(d)
      file = open(path, 'w')

   def close():
      if file:
	 file.close()
	 file = None

   def write(s):
      file.write(s)


class TweetSerializer:
   first = True
   ended = True
   count = 0
   maxTweets = 1
   store = None

   def __init__(self, store = TweetStore(), max = 1):
      self.store = store
      self.maxTweets = max

   def start(self):
      store.newFile()
      store.write("[\n")
      self.first = True
      self.ended = False

   def end(self):
      if not self.ended:
         self.store.write("\n]\n")
	 self.store.close()
	 self.first = False
	 self.ended = True

   def store(self, tweet):
      if self.ended:
	 self.start()

      if not self.first:
         self.store.write(",\n")
      self.first = False
      self.store.write(json.dumps(tweet._json).encode('utf8'))
      count += 1
      if count > maxTweets:
	 self.end()

#Import the necessary methods from tweepy library

#This is a basic listener that just prints received tweets to stdout.
class TweetWriter(tweepy.StreamListener):
   s = None

   def __init__(self, tweetSerializer = TweetSerializer()):
      self.s = tweetSerializer

   def on_data(self, data):
      s.write(data)
      return True

   def on_error(self, status):
      print("error from tweet stream: ", status, file=sys.stderr)
      return False

def interrupt(signum, frame):
   s.end()
   exit(1)

if __name__ == '__main__':

   execfile("./creds.py");
   auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
   auth.set_access_token(access_token, access_token_secret)

   signal.signal(signal.SIGINT, interrupt)

   api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
   s = TweetSerializer()
   w = TweetWriter(s)
   stream = tweepy.Stream(auth, w)

   # filter stream according to argv
   stream.filter(track=sys.argv)

   s.end()
