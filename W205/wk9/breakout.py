from __future__ import print_function
import sys
import tweepy
import datetime
import signal
import json
import time
import os

class TweetStore:
   seralizer = None
   maxTweets = 0
   maxSize = 0
   nTweets = 0
   nFiles = 0
   pathPattern = None
   _path = None
   file = None
   closing = False
   B = 1
   KB = 1000 * B
   MB = 1000 * KB
   GB = 1000 * MB
   TB = 1000 * GB

   def __init__(self, serializer = None, pathPattern = "%Y-%m-%d/tweets-%n", maxTweets = 1, maxSize = 0):
      if serializer == None:
         raise Exception('no serializer given')
      self.serializer = serializer
      self.pathPattern = pathPattern
      self.maxTweets = maxTweets
	   self.maxSize = maxSize

   def _nextPath():
      path = self._path
      while path == None or os.path.exists(path):
         self.nFiles += 1
         pat = self.pathPattern.replace("%n", str(self.nFiles))
         path = time.strftime(pat)
      self._path = path

   def _newFile(self):
      self.close()
      self._nextPath()
      d = os.path.dirname(self._path)
      if not os.path.exists(d):
         os.makedirs(d)
      print("new file: ", self._path)
      self.file = open(self._path, 'w')

   def close(self):
      if self.closing or self.file == None:
         return
      self.closing = True
      self.serializer.closing()
      self.file.close()
      if self.nTweets == 0:
         # no tweets => don't need this file
         os.remove(self._path)
         self.nFiles -= 1
      self.file = None
      self.closing = False

   def write(self, s):
      if file == None:
         self._newFile()
      self.file.write(s)

   def writeTweet(self,  tweet):
      if self.closing:
         raise Exception('cannot write a tweet to a file that is closing')
      nTweets += 1
      self.write(tweet)
	   if nTweets == maxTweets or self.file.tell() >= maxSize:
         self.close()


class TweetSerializer:
   first = None
   ended = None
   ending = None
   store = None

   def __init__(self, store = None):
      if store == None:
         store = TweetStore(serializer = self)
      self.store = store
      self.ended = True

   def start(self):
      self.store.write("[\n")
      self.first = True
      self.ended = False

   def end(self):
      if self.ending:
         return
      self.ending = True
      if not self.ended:
         self.store.write("\n]\n")
         self.store.close()
         self.first = False
      self.ended = True
      self.ending = False

   def write(self, tweet):
      if self.ended:
         self.start()

      if not self.first:
         self.store.write(",\n")
      self.first = False
      self.store.writeTweet(json.dumps(json.loads(tweet)
                                       , sort_keys=True
                                       , indent=4
                                       , separators=(',', ': ')).encode('utf8'))

   def closing(self):
      self.end()

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

# vim: tabstop=3 expandtab softtabstop=3
