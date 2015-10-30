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
   """
   Makes a method into one that's reentrant.

   It uses inheritance and a constructor to do so,
   so it's kinda ugly.
   """

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
   """
   Store tweets according to a policy.
   """
   serializer = None
   maxTweets = -1
   maxSize = -1
   nTweets = 0
   nFiles = 0
   pathPattern = None
   file = None
   _path = None
   _closing = False
   _substRe = re.compile('(%\d*n)')

   B = 1
   KB = 1000 * B
   MB = 1000 * KB
   GB = 1000 * MB
   TB = 1000 * GB

   def __init__(self, serializer = None, pathPattern = "%Y-%m-%d/tweets-%05n", maxTweets = None, maxSize = None):
      """
      Set policy of how tweets are stored.

      maxTweets   - max # of tweets per file
      maxSize     - tweets will be written to a new file
                    once current one exceeds this limit in bytes
      pathPattern - a pattern for how files containing tweets
                    will be named.  can contain %-directives.
                    %n indicate a file number, all others are
                    as in strftime, which see. a pattern like
                    "%Y-%m-%d/%04n" will put tweets in a file
                    named 2015-01-01/0001.  As time passes,
                    those files will move to 2015-01-02.
      """
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

   def _makePath(self, n):
      pat = self._substPctN(self.pathPattern)
      path = time.strftime(pat)
      return path


   def _nextPath(self):
      path = self._path
      while path == None or os.path.exists(path):
         self.nFiles += 1
         path = self._makePath(self.nFiles)
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
      """
      Close the store.

      A subsequent write to the store will re-open it.
      """
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
      """
      Write bytes to a tweet store.

      Typically, these bytes have to do with
      serialization.  Write tweets using the
      writeTweet() method.
      """
      if self.file == None:
         self._newFile()
      self.file.write(s)

   def writeTweet(self,  tweet):
      """
      Write a tweet to the store.
      """
      if self._closing:
         raise Exception('tweet file "%s" is closing, cannot write to it' % self._path)
      self.nTweets += 1
      sys.stdout.write('.')
      sys.stdout.flush()
      self.write(tweet)
      if self.maxTweets >= 0 and self.nTweets == self.maxTweets \
         or self.maxSize >= 0 and self.file.tell() >= self.maxSize \
         or self._path != self._makePath(self.nFiles):
         print("%d tweets, max %d; %d bytes, max %d" %
               (self.nTweets, self.maxTweets, self.file.tell(), self.maxSize))
         self.close()


class TweetSerializer(Reentrant):
   first = None
   ended = None
   store = None

   def __init__(self, store = None):
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
   stopped = False

   def __init__(self, tweetSerializer = None):
      self.s = tweetSerializer

   def on_data(self, data):
      s.write(data)
      return not self.stopped

   def on_disconnect(self, notice):
      print("disconnected", file=sys.stderr)
      s.end()

   def on_error(self, status):
      print("error from tweet stream: ", status, file=sys.stderr)
      s.end()
      return False

   def stop(self):
      self.stopped = True

def interrupt(signum, frame):
   w.stop()

if __name__ == '__main__':

   # Bring in twitter creds; this file is *not*
   # in source code control; you've got to provide
   # it yourself
   execfile("./creds.py");

   auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
   auth.set_access_token(access_token, access_token_secret)

   # Handle signals
   signal.signal(signal.SIGINT, interrupt)
   signal.signal(signal.SIGTERM, interrupt)

   api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
   st = TweetStore(maxTweets = 100, pathPattern='tweets/%Y-%m-%d/%05n')
   s = TweetSerializer(store = st)
   st.serializer = s
   w = TweetWriter(s)
   stream = tweepy.Stream(auth, w)

   # filter stream according to argv, in a separate thread
   stream.filter(track=sys.argv, async=True)

   # Pass the time, waiting for an interrupt
   while not w.stopped:
      time.sleep(10)
   stream.disconnect()
   s.end()

# vim: expandtab shiftwidth=3 softtabstop=3 tabstop=3
