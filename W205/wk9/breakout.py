from __future__ import print_function
import sys
import tweepy
import datetime
import signal
import json
import time
import os
import re

class Reentrant(object):
   _meth = None

   def __init__(self, meth):
      self._meth = meth
      setattr(self, self._meth.func_name, self._wrap)

   def _noop(self, *args, **kw_args):
      pass

   def _wrap(self, *args, **kw_args):
      setattr(self, self._meth.func_name, self._noop)
      self._meth(*args, **kw_args)
      setattr(self, self._meth.func_name, self._wrap)

class TweetStore(Reentrant):
   serializer = None
   maxTweets = -1
   maxSize = -1
   nTweets = 0
   nFiles = 0
   pathPattern = None
   _path = None
   file = None
   _closing = False
   _substRe = re.compile('(%\d*n)')
   B = 1
   KB = 1000 * B
   MB = 1000 * KB
   GB = 1000 * MB
   TB = 1000 * GB

   def __init__(self, serializer = None, pathPattern = "%Y-%m-%d/tweets-%05n", maxTweets = None, maxSize = None):
      self.serializer = serializer
      self.pathPattern = pathPattern
      if maxTweets != None:
         self.maxTweets = maxTweets
      if maxSize != None:
         self.maxSize = maxSize
      super(TweetStore, self).__init__(meth = self.close)

   def _substPctN(self, pat):
      m = self._substRe.search(pat)
      if m == None:
         return pat
      s = m.group().replace('n', 'd') % self.nFiles
      return self._substRe.sub(s, pat)

   def _nextPath(self):
      path = self._path
      while path == None or os.path.exists(path):
         self.nFiles += 1
         pat = self.pathPattern.replace("%n", str(self.nFiles))
         pat = self._substPctN(self.pathPattern)
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
      if self.file == None:
         return
      self._closing = True
      sys.stdout.write("\n")
      self.serializer.closing()
      self.file.close()
      if self.nTweets == 0:
         # no tweets => don't need this file
         os.remove(self._path)
         self.nFiles -= 1
      self.file = None
      self.nTweets = 0
      self._closing = False

   def write(self, s):
      if self.file == None:
         self._newFile()
      self.file.write(s)

   def writeTweet(self,  tweet):
      if self._closing:
         raise Exception('tweet file "%s" is closing, cannot write to it' % self._path)
      self.nTweets += 1
      sys.stdout.write('.')
      sys.stdout.flush()
      self.write(tweet)
      if self.maxTweets >= 0 and self.nTweets == self.maxTweets \
         or self.maxSize >= 0 and self.file.tell() >= self.maxSize:
         print("%d tweets, max %d; %d bytes, max %d" %
               (self.nTweets, self.maxTweets, self.file.tell(), self.maxSize))
         self.close()


class TweetSerializer(Reentrant):
   first = None
   ended = None
   store = None

   def __init__(self, store = None):
      if store == None:
         store = TweetStore(serializer = self)
      self.store = store
      self.ended = True
      super(self.__class__, self).__init__(meth = self.end)

   def start(self):
      self.store.write("[\n")
      self.first = True
      self.ended = False

   def end(self):
      if not self.ended:
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
      s.end()
      return False

def interrupt(signum, frame):
   s.end()
   exit(1)

if __name__ == '__main__':

   execfile("./creds.py");
   auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
   auth.set_access_token(access_token, access_token_secret)

   signal.signal(signal.SIGINT, interrupt)
   signal.signal(signal.SIGTERM, interrupt)
   signal.signal(signal.SIGQUIT, interrupt)

   api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
   st = TweetStore(maxTweets = 100)
   s = TweetSerializer(store = st)
   st.serializer = s
   #print("writing")
   #s.write('{ "eek": "a-mouse" }')
   #print("written")
   #exit(0)
   w = TweetWriter(s)
   stream = tweepy.Stream(auth, w)

   # filter stream according to argv
   stream.filter(track=sys.argv)

   s.end()

# vim: expandtab shiftwidth=3 softtabstop=3 tabstop=3
