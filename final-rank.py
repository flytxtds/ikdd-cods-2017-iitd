
# coding: utf-8

# In[1]:

import string
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import re
import csv
from tqdm import tqdm


# ### Tag Tweets as HI,EN,CME,CMH or CMEQ

# In[2]:

tweet_types = {}
meta = {}
with open('Datasheet.csv','r') as f:
    for x in f:
        tuples=x.split(',')
        en_count=0
        hi_count=0
        total_word_count=0
        other_count=0
        ne_count=0
        meta_data=[]
        for i in range(1, len(tuples)) :
            r = tuples[i].split(':')
            s_index = int(r[0])
            e_index = int(r[1])
            w_type = str(r[2])
            count = 1
            meta_data.append((s_index,e_index,w_type))
            if(w_type == 'EN'):
                en_count+=count
            elif w_type == 'HI' :
                hi_count+= count
            elif w_type == 'OTHER' :
                other_count+=count
            elif w_type == 'NE' :
                ne_count+=count
        meta[tuples[0]]=meta_data
        total_word_count=en_count+hi_count
            
        en_ratio = float(en_count)/float(total_word_count)
        hi_ratio = float(hi_count)/float(total_word_count)
        t='None'
        if(en_ratio>.9):
            t='EN'
        elif hi_ratio > .9:
            t='HI'
        elif hi_ratio > .5:
            t='CMH'
        elif en_ratio > .5:
            t='CME'
        elif en_ratio == .5:
            t='CMEQ'
        
        tweet_types[tuples[0]]= t


# In[3]:

filtered_tweets={}
with open('data.csv','rU') as f:
    reader = csv.reader(f, delimiter=',')
    for tweet_id,user_id,tweet in tqdm(list(reader)):
        filtered_words = []
        for s,e,t in meta[tweet_id]:
            word = tweet[s-1:e]
            if t=='HI' and word.lower().startswith('main'):
                continue
            filtered_words.append(word)
        filtered_tweets[tweet_id]=' '.join(filtered_words)


# ### Tokenize, filter stopwords and stem

# In[4]:

PUNCTUATION = set(string.punctuation)
STOPWORDS = set(stopwords.words('english'))
STEMMER = PorterStemmer()

# Function to break text into "tokens", lowercase them, remove punctuation and stopwords, and stem them
def tokenize(text):
    tokens = word_tokenize(re.sub('[^A-Za-z0-9]+', ' ', text))
    lowercased = [t.lower() for t in tokens]
    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if not w in STOPWORDS]
    stemmed = [STEMMER.stem(w) for w in no_stopwords]
    return [w for w in stemmed if w]


# ### Words to be ranked 
# 
# Read the words to be ranked from input.txt

# In[5]:

print 'Reading the words to be ranked from input.txt'
stemmed_key_words = []
key_words = []
with open('input.txt','rU') as f:
    reader = csv.reader(f)
    for key_word, in tqdm(list(reader)):
        key_words.append(key_word)
        stemmed_key_words.append(tokenize(key_word)[0])
        


# # Identify required HashTags

# In[6]:

req_hash_tags=set()
with open('data.csv','rU') as f:
    reader = csv.reader(f, delimiter=',')
    for tweet_id,user_id,tweet in tqdm(list(reader)):
        words = tweet.split(" ")
        hash_tags = []
        for word in words:
            word = word.lower()
            if word.startswith('#'):
                hash_tags.append(word)
        if len(hash_tags)>0:
            words = tokenize(tweet)
            for word in words:
                if word in stemmed_key_words:
                    for hash_tag in hash_tags:
                        req_hash_tags.add(hash_tag)
                    break


# # User Metric based on HI and CMH HashTags only

# In[7]:

word_users = {}
with open('data.csv','rU') as f:
    reader = csv.reader(f, delimiter=',')
    for tweet_id,user_id,tweet in tqdm(list(reader)):
        words = tweet.split(" ")
        hash_tag_present=False
        for word in words:
            if(word.startswith('#')):
                if word in req_hash_tags:
                    hash_tag_present=True
                    break
        
        if (tweet_types[tweet_id]!='HI' and tweet_types[tweet_id]!= 'CMH') and hash_tag_present==False:
            continue
        
        words = tokenize(filtered_tweets[tweet_id])
        
        for word in words:
            word = word.lower()
            if word in stemmed_key_words :
                tweet_type = tweet_types[tweet_id]
                if word not in word_users:
                    word_users[word] = {}                    
                if tweet_type not in word_users[word]:
                    word_users[word][tweet_type] = set([user_id])
                else:
                    word_users[word][tweet_type].add(user_id)


# In[8]:

user_metric_dict = {}
u_en_dict = {}
u_hi_dict = {}
u_cmh_dict = {}
for word,type_user_counts in word_users.items():
    u_en = 0.0
    if 'EN' in type_user_counts:
        u_en = float(len(type_user_counts['EN']))
    u_hi = 0.0
    if 'HI' in type_user_counts:
        u_hi = float(len(type_user_counts['HI']))
    u_cmh = 0.0
    if 'CMH' in type_user_counts:
        u_cmh = float(len(type_user_counts['CMH']))
    user_metric = ((u_hi+u_cmh)/u_en)
    u_en_dict[word] = u_en
    u_hi_dict[word] = u_hi
    u_cmh_dict[word] = u_cmh
    user_metric_dict[word] = user_metric


# # Tweet Metric based on Hindi and CMH Hash Tags

# In[9]:

word_tweets = {}
with open('data.csv','rU') as f:
    reader = csv.reader(f, delimiter=',')
    for tweet_id,user_id,tweet in tqdm(list(reader)):
        
        words = tweet.split(" ")
        hash_tag_present=False
        for word in words:
            if(word.startswith('#')):
                if word in req_hash_tags:
                    hash_tag_present=True
                    break
        
        if (tweet_types[tweet_id]!='HI' and tweet_types[tweet_id]!= 'CMH') and hash_tag_present==False:
            continue
        
        words = tokenize(filtered_tweets[tweet_id])

        for word in words:
            word = word.lower()
            if word in stemmed_key_words :
                tweet_type = tweet_types[tweet_id]
                if word not in word_tweets:
                    word_tweets[word] = {}                    
                if tweet_type not in word_tweets[word]:
                    word_tweets[word][tweet_type] = set([tweet_id])
                else:
                    word_tweets[word][tweet_type].add(tweet_id)


# In[10]:

tweet_metric_dict = {}
t_en_dict = {}
t_hi_dict = {}
t_cmh_dict = {}
for word,type_tweet_counts in word_tweets.items():
    t_en = 0.0
    if 'EN' in type_tweet_counts:
        t_en = float(len(type_tweet_counts['EN']))
    t_hi = 0.0
    if 'HI' in type_tweet_counts:
        t_hi = float(len(type_tweet_counts['HI']))
    t_cmh = 0.0
    if 'CMH' in type_tweet_counts:
        t_cmh = float(len(type_tweet_counts['CMH']))
    tweet_metric = ((t_hi+t_cmh)/t_en)
    t_en_dict[word] = t_en
    t_hi_dict[word] = t_hi
    t_cmh_dict[word] = t_cmh
    tweet_metric_dict[word] = tweet_metric


# ### Final metric calculation
# Final metric is calculated as mean of tweet metric and user metric

# In[11]:

final_ranks = []
final_rank_dict = {}
for key_word,stemmed_key_word in zip(key_words,stemmed_key_words):
    final_metric = (user_metric_dict[stemmed_key_word]+tweet_metric_dict[stemmed_key_word])/2
    final_ranks.append((key_word,final_metric))
    final_rank_dict[key_word]=final_metric


# ### Write output to the file

# In[ ]:

with open('output.csv','w') as f:
    writer = csv.writer(f)
    writer.writerows(final_ranks)

