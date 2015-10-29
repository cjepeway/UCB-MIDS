from __future__ import print_function
import sys
import tweepy
import datetime
import signal
import json
import time
import os

class TweetStore:
   fileCount = 0
   pathPattern = None
   file = None

   def __init__(self, pathPattern = "%Y-%m-%d/tweets-%n"):
      self.pathPattern = pathPattern

   def newFile(self):
      self.close()
      self.fileCount += 1
      path = "."
      while os.path.exists(path):
         self.fileCount += 1
         pat = self.pathPattern.replace("%n", str(self.fileCount))
         path = time.strftime(pat)
      d = os.path.dirname(path)
      if not os.path.exists(d):
         os.makedirs(d)
      print("new file: ", path)
      self.file = open(path, 'w')

   def close(self):
      if self.file:
         self.file.close()
         self.file = None

   def write(self,  s):
      self.file.write(s)


class TweetSerializer:
   first = None
   ended = None
   count = 0
   maxTweets = 1
   store = None

   def __init__(self, store = None, max = 1):
      if store == None:
         store = TweetStore()
      self.store = store
      self.maxTweets = max
      self.ended = True

   def start(self):
      self.store.newFile()
      self.store.write("[\n")
      self.first = True
      self.ended = False

   def end(self):
      if self.ended != None and not self.ended:
         self.store.write("\n]\n")
         self.store.close()
         self.first = False
      self.ended = True

   def write(self, tweet):
      if self.ended:
         self.start()

      if not self.first:
         self.store.write(",\n")
      self.first = False
      self.store.write(json.dumps(json.loads(tweet)
                                           , sort_keys=True
                                           , indent=4
                                           , separators=(',', ': ')).encode('utf8'))
      self.count += 1
      if self.count == self.maxTweets:
         self.end()
         self.count = 0
         sys.stdout.write("\n")
      sys.stdout.write(".")

class TweetWriter(tweepy.StreamListener):
   s = None

   def __init__(self, tweetSerializer = None):
      if tweetSerializer == None:
         tweetSerializer = TweetSerializer()
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
   s = TweetSerializer(max = 100)
   w = TweetWriter(s)
   stream = tweepy.Stream(auth, w)

   # filter stream according to argv
   stream.filter(track=sys.argv)

   s.end()
