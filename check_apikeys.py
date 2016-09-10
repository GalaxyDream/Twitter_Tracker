

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

from util import full_stack
import requests, concurrent.futures, os, json
import twython


def test_apikeys(apikeys):

    APP_KEY = apikeys['app_key']
    APP_SECRET = apikeys['app_secret']
    OAUTH_TOKEN = apikeys['oauth_token']
    OAUTH_TOKEN_SECRET = apikeys['oauth_token_secret']

    twitter = twython.Twython(APP_KEY, APP_SECRET, oauth_version=2)

    try:
        ACCESS_TOKEN = twitter.obtain_access_token()
        twitter = twython.Twython(APP_KEY, access_token=ACCESS_TOKEN)

        if (ACCESS_TOKEN):
            return True
        else:
            return False
    except Exception as exc:
        logger.info("apikeys [%s] failed: %s"%(apikeys, exc))
        return False


    

if __name__=="__main__":

    with open(os.path.abspath("config.json"), 'r') as config_f:
        config = json.load(config_f)

        for f0rmer in config['apikeys']:
            logger.info('testing... [%s]'%(f0rmer))
            is_valid = test_apikeys(config['apikeys'][f0rmer])
            print (is_valid)
            if( not is_valid):
                logger.info('[%s] is NOT valid'%(f0rmer))



                