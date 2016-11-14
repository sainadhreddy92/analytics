import storm
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
class SentimentBolt(storm.BasicBolt):

  def readfile(self,filename):
    res = []
    try:
      fp = open(filename,"r")
      for tweet in fp.read().split("\n"):
        res.append(tweet)
      fp.close()
    except IOError:
      return 0
    return res

  def calcprob(self,word_dict,word):
    nk = 0 if not word in word_dict else word_dict[word]
    vocab = 0
    for key in word_dict:
      vocab+=word_dict[key]
    unique_words = len(list(word_dict.keys()))

    #print "Vocab is",vocab
    #print "unique words are",unique_words
    #print "nk is ",nk
    prob = (nk+1)/((vocab+unique_words)*1.0)
    #print prob
    return prob



  def helper(self,posfile,negfile,tweet):

    postweets = self.readfile(posfile)
    negtweets = self.readfile(negfile)


    postweet_dict = {}
    negtweet_dict = {}

    for pos_tweet in postweets:
      for word in pos_tweet.split():
        if len(word)>=3:
          if word.lower() in postweet_dict:
            postweet_dict[word.lower()]+=1
          else:
            postweet_dict[word.lower()]=1

    for neg_tweet in negtweets:
      for word in neg_tweet.split():
        if len(word)>=3:
          if word.lower() in negtweet_dict:
            negtweet_dict[word.lower()]+=1
          else:
            negtweet_dict[word.lower()]=1

    prob_pos_tot = len(postweets)/((len(postweets)+len(negtweets))*1.0)
    prob_neg_tot = len(negtweets)/((len(postweets)+len(negtweets))*1.0)

    pos_prob  = prob_pos_tot
    neg_prob =  prob_neg_tot

    #print tweet

    for word in tweet.split():
      if len(word)>=3:
        pos_prob*=self.calcprob(postweet_dict,word)
        neg_prob*=self.calcprob(negtweet_dict,word)

    if neg_prob > pos_prob:
      fp = open(negfile,"a+")
      fp.write(tweet)
      fp.close()
      return "Negitive"
    else:
      fp = open(posfile,"a+")
      fp.write(tweet)
      fp.close()
      return "Positive"

  def process(self, tup):
    tweet_tuple = tup.values[0]
    storm.emit ([tweet_tuple+SentimentBolt().helper("pos_data.txt","neg_data.txt",tweet_tuple)])

SentimentBolt().run()
