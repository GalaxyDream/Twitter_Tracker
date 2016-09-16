#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

import os, json, sys
import csv
import codecs
import time
import re
from util import full_stack, chunks, md5
#from langdetect import detect
#from ftfy import fix_text
#import nltk
#from nltk import word_tokenize

#from geocoding.tweet_us_geocoder import TweetUSGeocoder

def eliminate_duplicates_worker(tweets_filename):
    local_tweet_ids = set()
    with open(tweets_filename, 'r') as rf, open('%s.new'%(tweets_filename), 'a+') as wf:

        total = 0
        duplicate = 0
        keep = 0
        for line in rf:

            tweet = json.loads(line)

            total += 1

            tweet_id = int(tweet['id'])

            if(tweet_id in local_tweet_ids):
                duplicate += 1
                continue

            local_tweet_ids.add(tweet_id)
            wf.write('%s\n'%json.dumps(tweet))
            keep += 1

        logger.info('total: [%d]; duplicate: [%d]; keep: [%d]'%(total, duplicate, keep))


def eliminate_duplicates(tweets_folder):

    for root, dirs, files in os.walk(os.path.abspath(tweets_folder)):
        for f in files:
            logger.info(f)
            if (f == 'search.json' or f == '.DS_Store'):
                continue
            #search_config = find_search_config_by_output_filename(f, search_configs)
            
            eliminate_duplicates_worker(os.path.join(root, f))

if __name__ == "__main__":
    
    logger.info(sys.version)

    eliminate_duplicates_worker('/Volumes/DATA2/twitterlab/twittertracker/data/tweets/sample/2015-01/20150118.json')

