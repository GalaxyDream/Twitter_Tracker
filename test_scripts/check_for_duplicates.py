#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

import os, json, time

def check_for_duplicates(folder_name):
    
    for root, dirs, files in os.walk(os.path.abspath(folder_name)):
        for f in files:
            if (f == 'users.json' or f.startswith('.')):
                continue

            tweet_ids = set()
            logger.info(f)
            with open(os.path.join(root, f), 'r') as rf:
                for line in rf:
                    
                    tweet = json.loads(line)
                    tweet_id = tweet['id']
                    if (tweet_id in tweet_ids):
                        logger.error('duplicated: [%d]'%(tweet_id))

                    tweet_ids.add(tweet_id)

                    #logger.info('ID: %d; TEXT: %s'%(tweet['id'], tweet['text']))


if __name__=="__main__":
    check_for_duplicates(os.path.abspath('./data/users_timeline'))




