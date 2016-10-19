#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
twitter_streamer.py: 

KeywordsStreamer: straightforward class that tracks a list of keywords; most of the jobs are done by TwythonStreamer; the only thing this is just attach a WriteToHandler so results will be saved

'''

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

import sys, time, argparse, json, os, pprint, datetime
import twython
from util import full_stack, chunks, md5

class TwitterStreamer(twython.TwythonStreamer):

    def __init__(self, APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, output_folder='./data'):

        self.output_folder = os.path.abspath(output_folder)
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        self.counter = 0

        super(TwitterStreamer, self).__init__(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    def on_success(self, tweet):

        if 'text' in tweet:
            now = datetime.datetime.now()
            month_folder = os.path.abspath('%s/%s'%(self.output_folder, now.strftime('%Y-%m')))
            if not os.path.exists(month_folder):
                os.makedirs(month_folder)

            output_file = os.path.abspath('%s/%s.json'%(month_folder, now.strftime("%Y%m%d")))
            
            with open(output_file, 'a+') as f:
                f.write('%s\n'%json.dumps(tweet))

                self.counter += 1
                if self.counter % 10000 == 0:
                    logger.info("received: %d"%self.counter)
            
    def on_error(self, status_code, data):
         logger.warn('ERROR CODE: [%s]-[%s]'%(status_code, data))
        
    def close(self):
        self.disconnect()

def init_streamer(config, output_folder):
    
    apikeys = list(config['apikeys'].values()).pop()

    # APP_KEY = '9ykLAyaYDtBDtg7pg6Uow' #apikeys['app_key']
    # APP_SECRET = '4jVSiBJpLOgQ5WnchdMfziwt4ihGjg1RXrLSwL7tw' #apikeys['app_secret']
    # OAUTH_TOKEN = '142106761-1NKl8UoB6968caQdUb2Xx8Gyd27YSeA41pfFAjm4' #apikeys['oauth_token']
    # OAUTH_TOKEN_SECRET = 'jh0liDAENolXg5bPTTp35BBg1FJaMjf0y9ZSo4NKUr0' #apikeys['oauth_token_secret']

    APP_KEY = apikeys['app_key']
    APP_SECRET = apikeys['app_secret']
    OAUTH_TOKEN = apikeys['oauth_token']
    OAUTH_TOKEN_SECRET = apikeys['oauth_token_secret']

    streamer = TwitterStreamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, output_folder=output_folder)

    return streamer


def collect_public_tweets(config, output_folder):

    streamer = init_streamer(config, output_folder)

    logger.info("start collecting.....")

    streamer.statuses.sample()

def filter_by_locations(config, output_folder, locations = None):

    with open(os.path.abspath(locations), 'r') as locations_f:

        geo_locations = json.load(locations_f)

        name = geo_locations['name']
        locations = geo_locations['locations']

        streamer = init_streamer(config, '%s/%s'%(output_folder,name))
    
        logger.info("start collecting for %s....."%(name))

    # if (locations and locations.endswith('.json')):
    #     with open(os.path.abspath(locations), 'r') as locations_f:
    #         locations = json.load(locations_f)
    #         locations = ','.join([','.join([str(g) for g in pair]) for pair in locations['bounding_box']])

        streamer.statuses.filter(locations=locations)


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="config.json that contains twitter api keys;", default="./config.json")
    parser.add_argument('-o','--output', help="output folder data", default="./data/")
    parser.add_argument('-cmd','--command', help="command", default="sample")
    parser.add_argument('-cc','--command_data', help="command data", default=None)

    args = parser.parse_args()


    with open(os.path.abspath(args.config), 'r') as config_f:
        config = json.load(config_f)

        try:
            while(True):
                try:
                    if (args.command == 'locations'):
                        filter_by_locations(config, args.output, args.command_data)
                    else:
                        collect_public_tweets(config, args.output)
                except Exception as exc:        
                    logger.error(exc)
                    #logger.error(full_stack())
                
                time.sleep(10)
                logger.info("restarting...")
        except KeyboardInterrupt:
            print()
            logger.error('You pressed Ctrl+C!')
            pass
        finally:
            pass
