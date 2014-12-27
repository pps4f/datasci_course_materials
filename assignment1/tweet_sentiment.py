import sys
import json

sentScores = {} # initialize an empty dictionary
tweetsList = [] # initialize an empty list to store Tweets and sentiment scores

def hw():
  print 'Hello, world!'

def lines(fp):
  print str(len(fp.readlines()))

def build_sentiment_dict(fp):
  for line in fp:
    term, score  = line.split("\t")  # The file is tab-delimited. "\t" means "tab character"
    sentScores[term] = int(score)  # Convert the score to an integer.
  # print scores.items() # Print every (term, score) pair in the dictionary

def get_sent_score(tweetStr):
  if tweetStr is None:
    return 0
  tweetList = tweetStr.split()  
  tweetScore = 0
  for token in tweetList:
    score = sentScores.get(token.lower())
    if score is not None:
      tweetScore += score
  print tweetScore
  return tweetScore

def build_tweets_dict(fp):
  count = 0
  for line in fp:
    tweetJson = json.loads(line)
    count += 1
    value = tweetJson.get('text')
    print count, value, 
    tweetsList.append({'text':value, 'sentScore':get_sent_score(value)})

def main():
  sent_file = open(sys.argv[1])
  tweet_file = open(sys.argv[2])
  # hw()
  # lines(sent_file)
  # lines(tweet_file)
  build_sentiment_dict(sent_file)
  build_tweets_dict(tweet_file)
  print len(tweetsList)

if __name__ == '__main__':
  main()
