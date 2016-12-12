    #!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import logging.handlers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

import sys, time, argparse, json, os, pprint, datetime, copy
import twython
from util import full_stack, chunks, md5
from proxy_check import check_proxy_twython, proxy_checker, check_proxy
from exceptions import NotImplemented, MissingArgs, WrongArgs, InvalidConfig, MaxRetryReached
import concurrent.futures
import functools
import multiprocessing as mp
import itertools
import signal
import re
import pandas as pd
#sys.path.append(".")

MAX_RETRY_CNT = 3
WAIT_TIME = 30

class TwitterCrawler(twython.Twython):

    def __init__(self, *args, **kwargs):
        """
        Constructor with apikeys, and output folder

        * apikeys: apikeys
        """
        import copy

        apikeys = copy.copy(kwargs.pop('apikeys', None))

        if not apikeys:
            raise MissingArgs('apikeys is missing')

        self.apikeys = copy.copy(apikeys) # keep a copy
        #self.crawler_id = kwargs.pop('crawler_id', None)

        self.output_folder = kwargs.pop('output_folder', './data')
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        oauth2 = kwargs.pop('oauth2', True) # default to use oauth2 (application level access, read-only)

        if oauth2:
            apikeys.pop('oauth_token')
            apikeys.pop('oauth_token_secret')
            twitter = twython.Twython(apikeys['app_key'], apikeys['app_secret'], oauth_version=2)
            access_token = twitter.obtain_access_token()
            kwargs['access_token'] = access_token
            apikeys.pop('app_secret')

        kwargs.update(apikeys)

        super(TwitterCrawler, self).__init__(*args, **kwargs)
    def fetch_geo(self, query = None, now=datetime.datetime.now()):

        if not query:
            raise Exception("Cityname: NAME cannot be None")

        day_output_folder = os.path.abspath('%s/%s'%(self.output_folder, now.strftime('%Y%m%d')))

        if not os.path.exists(day_output_folder):
            os.makedirs(day_output_folder)

        filename = os.path.abspath('%s/%s'%(day_output_folder, 'county_geo'))

        # with open(filename, 'w') as f:
        #     pass
        retry_cnt = MAX_RETRY_CNT
        while retry_cnt > 0:
            try:
                logger.info("going to get geo %s" %query)
                result = self.search_geo(query=query)
                logger.info(result)
                with open(filename, 'a+') as f:

                    f.write('%s\n'%json.dumps(result))

                time.sleep(1)
                
                return False

            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('geo', '/geo/search')
            except Exception as exc:
                time.sleep(10)
                logger.error("exception: %s; when fetching city: %d"%(exc, query))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    logger.warn("exceed max retry... return")
                    return False

        return False

    def rate_limit_error_occured(self, resource, api):
        rate_limits = self.get_application_rate_limit_status(resources=[resource])

        #e.g., ['resources']['followers']['/followers/list']['reset']

        wait_for = int(rate_limits['resources'][resource][api]['reset']) - time.time() + 10

        #logger.debug(rate_limits)
        logger.warn('[%s] rate limit reached, sleep for %d'%(rate_limits['rate_limit_context'], wait_for))
        if wait_for < 0:
            wait_for = 60

        time.sleep(wait_for)

    def fetch_users(self, parameter = 'screen_name', parameter_values = [], filename=None):
        '''
        call: /users/lookup
        '''
        if not parameter_values:
            raise Exception("users/lookup: parameter_values cannot be empty")

        if len(parameter_values) > 100:
            raise Exception("users/lookup: parameter_values cannot exceed 100 elements")

        #chunks = [parameter_values[x:x+100] for x in range(0, len(parameter_values), 100)]
        if (not filename):
            now = datetime.datetime.now()
            filename = os.path.abspath('%s/%s'%(self.output_folder, now.strftime('%Y%m%d%H%M%S')))
        else:
            filename = os.path.abspath('%s/%s'%(self.output_folder, filename))

        retry_cnt = 0
        while retry_cnt > 0:
            try:

                if (parameter == 'screen_name'):
                    result = self.lookup_user(screen_name=",".join(str(x) for x in parameter_values))
                elif (parameter == 'user_id'):
                    result = self.lookup_user(user_id=",".join(str(x) for x in parameter_values))

                if (result):

                    with open(filename, 'a+') as f:
                        f.write('%s\n'%json.dumps(result))

                        retry_cnt = 0

                time.sleep(1)

            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('users', '/users/lookup')
            except Exception as exc:
                time.sleep(10)
                logger.error("exception: %s; when fetching users"%(exc))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    logger.warn("exceed max retry... return")
                    return False

        return False

    def fetch_user_relationships(self, user_id = None, resource_family='friends', call='/friends/ids', now=datetime.datetime.now()):
        '''
        call: /friends/ids, /friends/list, /followers/ids, and /followers/list
        '''
        if not user_id:
            raise Exception("user_relationship: user_id cannot be None")

        day_output_folder = os.path.abspath('%s/%s'%(self.output_folder, now.strftime('%Y%m%d')))

        if not os.path.exists(day_output_folder):
            os.makedirs(day_output_folder)

        filename = os.path.abspath('%s/%s'%(day_output_folder, user_id))

        with open(filename, 'w') as f:
            pass

        cursor = -1

        cnt = 0

        retry_cnt = MAX_RETRY_CNT
        while cursor != 0 and retry_cnt > 0:
            try:
                result = None
                if (call == '/friends/ids'):
                    result = self.get_friends_ids(user_id=user_id, cursor=cursor,count=5000)

                    cnt += len(result['ids'])
                elif (call == '/friends/list'):

                    result = self.get_friends_list(user_id=user_id, cursor=cursor,count=200)

                    cnt += len(result['users'])
                elif (call == '/followers/ids'):

                    result = self.get_followers_ids(user_id=user_id, cursor=cursor,count=5000)

                    cnt += len(result['ids'])
                elif (call == '/followers/list'):

                    result = self.get_followers_list(user_id=user_id, cursor=cursor,count=200)

                    cnt += len(result['users'])
                
                if (result):

                    cursor = result['next_cursor'] 

                    with open(filename, 'a+') as f:
                        f.write('%s\n'%json.dumps(result))


                time.sleep(1)


            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured(resource_family, call)
            except Exception as exc:
                time.sleep(10)
                logger.error("exception: %s; when fetching user_id: %d"%(exc, user_id))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    logger.warn("exceed max retry... return")
                    return False

        logger.info("[%s] total [%s]: %d; "%(user_id, call, cnt))
        return False

    def fetch_retweets(self, tweet_id = None, now=datetime.datetime.now()):
        '''
        call: /friends/ids, /friends/list, /followers/ids, and /followers/list
        '''
        # print(type(tweet_id))
        
        if not tweet_id:
            raise Exception("retweet: retweet_id cannot be None")

        retweet_ids = set()

        day_output_folder = os.path.abspath('%s/%s'%(self.output_folder, now.strftime('%Y%m%d')))

        if not os.path.exists(day_output_folder):
            os.makedirs(day_output_folder)

        filename = os.path.abspath('%s/%s'%(day_output_folder, tweet_id))

        with open(filename, 'w') as f:
            pass
        retry_cnt = MAX_RETRY_CNT
        while retry_cnt > 0:
            try:
                result = self.get_retweets(id=tweet_id, count=100, trim_user = 1)
                logger.info("find %d retweets of [%d]"%(len(result), tweet_id))
                for tweet in result:
                    retweet_ids.add(tweet['id'])

                if(len(result) > 0):
                    with open(filename, 'a+') as f:

                        f.write('%s\n'%json.dumps(result))

                time.sleep(1)

                return False, retweet_ids

            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('statuses', '/statuses/retweets/:id')
            except Exception as exc:
                time.sleep(10)
                logger.error("exception: %s; when fetching tweet_id: %d"%(exc, tweet_id))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    logger.warn("exceed max retry... return")
                    return False, retweet_ids

        return False, retweet_ids

    def fetch_user_timeline(self, user_id = None,  now=datetime.datetime.now(), since_id = 1):

        if not user_id:
            raise Exception("user_timeline: user_id cannot be None")

        day_output_folder = os.path.abspath('%s/%s'%(self.output_folder, now.strftime('%Y%m%d')))

        if not os.path.exists(day_output_folder):
            os.makedirs(day_output_folder)

        filename = os.path.abspath('%s/%s'%(day_output_folder, user_id))

        prev_max_id = -1
        current_max_id = 0
        current_since_id = since_id

        cnt = 0

        retry_cnt = MAX_RETRY_CNT
        while current_max_id != prev_max_id and retry_cnt > 0:
            try:
                if current_max_id > 0:
                    tweets = self.get_user_timeline(user_id=user_id, since_id = since_id, max_id=current_max_id - 1, count=200)
                else:
                    tweets = self.get_user_timeline(user_id=user_id, since_id = since_id, count=200)

                prev_max_id = current_max_id # if no new tweets are found, the prev_max_id will be the same as current_max_id

                with open(filename, 'a+') as f:
                    for tweet in tweets:
                        f.write('%s\n'%json.dumps(tweet))
                        if current_max_id == 0 or current_max_id > int(tweet['id']):
                            current_max_id = int(tweet['id'])
                        if current_since_id == 0 or current_since_id < int(tweet['id']):
                            current_since_id = int(tweet['id'])

                #no new tweets found
                if (prev_max_id == current_max_id):
                    break;

                #timeline.extend(tweets)

                cnt += len(tweets)

                # if (cnt % 100):
                #     logger.info("received: [%d] for user: [%d]"%(cnt, user_id))

                #logger.debug('%d > %d ? %s'%(prev_max_id, current_max_id, bool(prev_max_id > current_max_id)))

                time.sleep(1)

            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('statuses', '/statuses/user_timeline')
            except Exception as exc:
                time.sleep(10)
                logger.error("exception: %s; when fetching user_id: %d"%(exc, user_id))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    logger.warn("exceed max retry... return")
                    return since_id, False, True

        logger.info("[%s] total tweets: %d; since_id: [%d]"%(user_id, cnt, since_id))
        return current_since_id, False, False

    def search_by_query(self, query, since_id = 0, geocode=None, lang=None, now=datetime.datetime.now(), output_filename = None):

        if not query:
            raise Exception("search: query cannot be None")

        #logger.info("query: %s; since_id: %d"%(query, since_id))

        day_output_folder = os.path.abspath('%s/%s'%(self.output_folder, now.strftime('%Y%m%d')))

        if not os.path.exists(day_output_folder):
            os.makedirs(day_output_folder)

        filename = os.path.abspath('%s/%s'%(day_output_folder, output_filename)) if output_filename else os.path.abspath('%s/%s'%(day_output_folder, int(time.time())))

        prev_max_id = -1

        current_max_id = 0
        cnt = 0
        current_since_id = since_id

        retry_cnt = MAX_RETRY_CNT
        #result_tweets = []
        while current_max_id != prev_max_id and retry_cnt > 0:
            try:
                if current_max_id > 0:
                    tweets = self.search(q=query, geocode=geocode, since_id=since_id, lang=lang, max_id=current_max_id-1, result_type='recent', count=100)
                else:
                    tweets = self.search(q=query, geocode=geocode, since_id=since_id, lang=lang, result_type='recent', count=100)


                prev_max_id = current_max_id # if no new tweets are found, the prev_max_id will be the same as current_max_id

                with open(filename, 'a+') as f:
                    for tweet in tweets['statuses']:
                        f.write('%s\n'%json.dumps(tweet))
                        if current_max_id == 0 or current_max_id > int(tweet['id']):
                            current_max_id = int(tweet['id'])
                        if current_since_id == 0 or current_since_id < int(tweet['id']):
                            current_since_id = int(tweet['id'])

                #no new tweets found
                if (prev_max_id == current_max_id):
                    break;

                #result_tweets.extend(tweets['statuses'])

                cnt += len(tweets['statuses'])

                # if (cnt % 1000 == 0):
                #     logger.info("[%d] tweets... "%cnt)

                #logger.info(cnt)

                #logger.debug('%d > %d ? %s'%(prev_max_id, current_max_id, bool(prev_max_id > current_max_id)))

                time.sleep(1)

            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('search', '/search/tweets')
            except Exception as exc:
                time.sleep(10)
                logger.error("exception: %s"%exc)
                retry_cnt -= 1
                if (retry_cnt == 0):
                    logger.warn("exceed max retry... return")
                    return since_id, False
                    #raise MaxRetryReached("max retry reached due to %s"%(exc))

        logger.info("[%s]; since_id: [%d]; total tweets: %d "%(query, since_id, cnt))
        return current_since_id, False

    def match_year(self, time_string = ''):

        match = re.findall('\d{4}', time_string)
        year = match[1] if match else 'Date'
        return year

    def lookup_history(self, tweets_id = None,  now=datetime.datetime.now()):


        if not tweets_id:
            raise Exception("tweets_history: tweets_id cannot be None")

        cnt = 0


        try:
            tweets = self.lookup_status(id=tweets_id)
            tweet_time = tweets[0]["created_at"]
            year = self.match_year(tweet_time)

            day_output_folder = os.path.abspath('%s/%s/%s'%(self.output_folder, now.strftime('%Y%m%d'), year))

            if not os.path.exists(day_output_folder):
                os.makedirs(day_output_folder)


            for tweet in tweets:
                filename = os.path.abspath('%s/%s'%(day_output_folder, tweet['id']))
                with open(filename, 'a+') as f:
                    for tweet in tweets:
                        f.write('%s\n'%json.dumps(tweet))

            cnt += len(tweets)
            time.sleep(5)

        except twython.exceptions.TwythonRateLimitError:
            self.rate_limit_error_occured('search', '/search/tweets')
        except Exception as exc:
            time.sleep(10)
            logger.error("exception: %s; when fetching tweet_id: %d"%(exc, tweets_id[0]))

        logger.info("total tweets: %d; since_id: [%d]"%(cnt, tweets_id[0]))
        return False

def generate_apikey_proxy_pair(apikeys_list, proxies_list):

    n_apikeys = len(apikeys_list)
    n_proxies = len(proxies_list)

    if n_apikeys > n_proxies:
        for apikeys_name, proxies in zip(apikeys_list, proxies_list):
            if apikeys_name and proxies:
                yield apikeys_name, [proxies]
    else:
        n_c = int(n_proxies / n_apikeys)
        for apikeys_name, proxies in zip(apikeys_list, chunks(proxies_list, n_c)):
            yield apikeys_name, proxies

def apikey_proxy_pairs(apikeys_list, proxies_list):
    apikey_proxy_pairs = {}

    if (len(proxies_list) > 0):

        for apikeys_name, proxies in generate_apikey_proxy_pair(apikeys_list, proxies_list):
            apikey_proxy_pairs[apikeys_name] = {
                "apikeys": apikeys_list[apikeys_name],
                "proxies": proxies
            }
    else:
        apikeys_name = list(apikeys_list.keys()).pop()
        apikey_proxy_pairs[apikeys_name] = {
            "apikeys": apikeys_list[apikeys_name],
            "proxies": []
        }

    return apikey_proxy_pairs

def fetch_users_worker(parameter, chunk, output_folder, filename, available, apikey_proxy_pairs_dict):

    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    apikeys = copy.copy(apikey_proxy_pairs_dict[available]['apikeys'])
    proxies = copy.copy(apikey_proxy_pairs_dict[available]['proxies'])

    proxies = iter(proxies) if proxies else None

    logger.info('REQUEST -> (chunk size: [%d])'%(len(chunk)))

    client_args = {"timeout": 30}

    retry = True
    try:
        while(retry):
            if proxies:
                proxy = next(proxies)
                logger.info('checking [%s]'%proxy)
                passed, proxy = check_proxy_twython(proxy['proxy'], 5)
                if not passed:
                    logger.warn('proxy failed, retry next one')
                    continue
                client_args['proxies'] = proxy['proxy_dict']

            twitterCralwer = TwitterCrawler(apikeys=apikeys, client_args=client_args, output_folder = output_folder)
            retry = twitterCralwer.fetch_users(parameter=parameter, parameter_values=chunk, filename=filename)
            logger.info("retry: %s"%(retry))
    # except StopIteration as exc:
    #     pass
    except Exception as exc:
        logger.error(exc)
        pass


    return available

def fetch_users_worker_done(future, available_apikey_proxy_pairs = []):

    available = future.result()

    logger.info('finished... [%s]'%available)
    available_apikey_proxy_pairs.append(available)


def collect_users(parameter, users_config_filename, output_folder, config, n_workers = mp.cpu_count(), proxies = []):

    apikey_proxy_pairs_dict = apikey_proxy_pairs(config['apikeys'], proxies)

    available_apikey_proxy_pairs = list(apikey_proxy_pairs_dict.keys())

    max_workers = len(available_apikey_proxy_pairs)

    #max_workers = mp.cpu_count() if max_workers > mp.cpu_count() else max_workers

    users_config = []
    with open(os.path.abspath(users_config_filename), 'r') as users_config_rf:
        users_config = list(set(json.load(users_config_rf)))

    chunks = [users_config[x:x+100] for x in range(0, len(users_config), 100)]

    max_workers = max_workers if max_workers < len(users_config) else len(users_config)
    max_workers = n_workers if n_workers < max_workers else max_workers
    logger.info("concurrent workers: [%d]"%(max_workers))

    futures_ = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:

        try:

            for chunk in chunks:

                while(len(available_apikey_proxy_pairs) == 0):
                    logger.info('no available_apikey_proxy_pairs, wait for %ds to retry...'%WAIT_TIME)
                    time.sleep(WAIT_TIME)

                time.sleep(1)

                now = datetime.datetime.now()
                filename = now.strftime('%Y%m%d%H%M%S')
                future_ = executor.submit(
                            fetch_users_worker, parameter, chunk, output_folder, filename, available_apikey_proxy_pairs.pop(), apikey_proxy_pairs_dict)

                future_.add_done_callback(functools.partial(fetch_users_worker_done, available_apikey_proxy_pairs=available_apikey_proxy_pairs))

                futures_.append(future_)
            else:
                concurrent.futures.wait(futures_)
                executor.shutdown()
                return False

        except KeyboardInterrupt:
            logger.warn('You pressed Ctrl+C! But we will wait until all sub processes are finished...')
            concurrent.futures.wait(futures_)
            executor.shutdown()
            raise

def fetch_retweets_worker(tweet_id, now, output_folder, available, apikey_proxy_pairs_dict):

    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    apikeys = copy.copy(apikey_proxy_pairs_dict[available]['apikeys'])
    proxies = copy.copy(apikey_proxy_pairs_dict[available]['proxies'])

    proxies = iter(proxies) if proxies else None

    logger.info('REQUEST -> (tweet_id: [%d])'%(tweet_id))

    client_args = {"timeout": 60}

    retry = True
    retweet_ids = set()
    try:
        while(retry):
            if proxies:
                proxy = next(proxies)
                logger.info('checking [%s]'%proxy)
                passed, proxy = check_proxy_twython(proxy['proxy'], 5)
                if not passed:
                    logger.warn('proxy failed, retry next one')
                    continue
                client_args['proxies'] = proxy['proxy_dict']

            twitterCralwer = TwitterCrawler(apikeys=apikeys, client_args=client_args, output_folder = output_folder)
            retry, retweet_ids = twitterCralwer.fetch_retweets(tweet_id, now=now)
            logger.info("retry: %s"%(retry))
    # except StopIteration as exc:
    #     pass
    except Exception as exc:
        logger.error(exc)
        pass


    return available, retweet_ids

def fetch_retweets_worker_done(future, available_apikey_proxy_pairs = [], retweet_ids = set()):

    available, this_retweet_ids = future.result()

    logger.info('finished... [%s]'%available)
    available_apikey_proxy_pairs.append(available)
    retweet_ids |= this_retweet_ids



def fetch_user_relationships_worker(user_id, resource_family, call, now, output_folder, available, apikey_proxy_pairs_dict):

    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    apikeys = copy.copy(apikey_proxy_pairs_dict[available]['apikeys'])
    proxies = copy.copy(apikey_proxy_pairs_dict[available]['proxies'])

    proxies = iter(proxies) if proxies else None

    logger.info('REQUEST -> (user_id: [%d]; call: [%s])'%(user_id, call))

    client_args = {"timeout": 30}

    retry = True
    remove = False
    try:
        while(retry):
            if proxies:
                proxy = next(proxies)
                logger.info('checking [%s]'%proxy)
                passed, proxy = check_proxy_twython(proxy['proxy'], 5)
                if not passed:
                    logger.warn('proxy failed, retry next one')
                    continue
                client_args['proxies'] = proxy['proxy_dict']

            twitterCralwer = TwitterCrawler(apikeys=apikeys, client_args=client_args, output_folder = output_folder)
            retry = twitterCralwer.fetch_user_relationships(user_id, resource_family=resource_family, call=call, now=now)
            logger.info("retry: %s"%(retry))
    # except StopIteration as exc:
    #     pass
    except Exception as exc:
        logger.error(exc)
        pass


    return available

def fetch_user_relationships_worker_done(future, available_apikey_proxy_pairs = []):

    available = future.result()

    logger.info('finished... [%s]'%available)
    available_apikey_proxy_pairs.append(available)

def collect_user_relatinoships_by_user_ids(call, user_ids_config_filename, output_folder, config, n_workers = mp.cpu_count(), proxies = []):

    apikey_proxy_pairs_dict = apikey_proxy_pairs(config['apikeys'], proxies)

    available_apikey_proxy_pairs = list(apikey_proxy_pairs_dict.keys())

    max_workers = len(available_apikey_proxy_pairs)

    #max_workers = mp.cpu_count() if max_workers > mp.cpu_count() else max_workers

    user_ids = []
    with open(os.path.abspath(user_ids_config_filename), 'r') as user_ids_config_rf:
        user_ids = json.load(user_ids_config_rf)

    user_ids = set(user_ids)

    logger.info("tracking [%d] users' [%s]"%(len(user_ids), call))

    max_workers = max_workers if max_workers < len(user_ids) else len(user_ids)
    max_workers = n_workers if n_workers < max_workers else max_workers
    logger.info("concurrent workers: [%d]"%(max_workers))

    resource_family = None
    m = re.match(r'^\/(?P<resource_family>.*?)\/', call)
    if (m):
        resource_family = m.group('resource_family')

    futures_ = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:

        try:

            for user_id in user_ids:

                while(len(available_apikey_proxy_pairs) == 0):
                    logger.info('no available_apikey_proxy_pairs, wait for %ds to retry...'%WAIT_TIME)
                    time.sleep(WAIT_TIME)


                now = datetime.datetime.now()
                future_ = executor.submit(
                            fetch_user_relationships_worker, user_id, resource_family, call, now, output_folder, available_apikey_proxy_pairs.pop(), apikey_proxy_pairs_dict)

                future_.add_done_callback(functools.partial(fetch_user_relationships_worker_done, available_apikey_proxy_pairs=available_apikey_proxy_pairs))

                futures_.append(future_)
            else:
                concurrent.futures.wait(futures_)
                executor.shutdown()
                return False

        except KeyboardInterrupt:
            logger.warn('You pressed Ctrl+C! But we will wait until all sub processes are finished...')
            concurrent.futures.wait(futures_)
            executor.shutdown()
            raise

def collect_retweets_by_tweets_ids(output_folder = None, config = None, tweets_ids = set(), n_workers = 1, proxies = None, level = -1):

        apikey_proxy_pairs_dict = apikey_proxy_pairs(config['apikeys'], proxies)

        available_apikey_proxy_pairs = list(apikey_proxy_pairs_dict.keys())

        max_workers = len(available_apikey_proxy_pairs)

        logger.info("tracking[%d] tweet" %(len(tweets_ids)))

        max_workers = max_workers if max_workers < len(tweets_ids) else len(tweets_ids)
        max_workers = n_workers if n_workers < max_workers else max_workers
        logger.info("concurrent workers: [%d]" %(max_workers))

        retweet_ids = set()
        futures_ = []

        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:

            try:


                for tweet_id in tweets_ids:

                    while(len(available_apikey_proxy_pairs) == 0):
                        logger.info('no available_apikey_proxy_pairs, wait for %ds to retry...'%WAIT_TIME)
                        time.sleep(WAIT_TIME)

                    now = datetime.datetime.now()
                    future_ = executor.submit(
                                fetch_retweets_worker, tweet_id, now, output_folder, available_apikey_proxy_pairs.pop(), apikey_proxy_pairs_dict)

                    future_.add_done_callback(functools.partial(fetch_retweets_worker_done, available_apikey_proxy_pairs=available_apikey_proxy_pairs, retweet_ids = retweet_ids))

                    futures_.append(future_)
                else:
                    # logger.info('finished one layer')
                    concurrent.futures.wait(futures_)
                    executor.shutdown()
                    level -= 1
                    logger.info("[RETWEETS]: working on level %s"%(level))
                    if ((level == 0) or (len(retweet_ids) == 0)):
                        return False
                    #elif ((level > 0 or level < 0) and len(retweet_ids) > 0):
                    else:
                        collect_retweets_by_tweets_ids(output_folder = output_folder, config = config, tweets_ids = retweet_ids, n_workers = n_workers, proxies = proxies, level = level)

                return False

            except KeyboardInterrupt:
                logger.warn('You pressed Ctrl+C! But we will wait until all sub processes are finished...')
                concurrent.futures.wait(futures_)
                executor.shutdown()
                raise

        return False

def collect_retweets (input_filename, output_folder, config, n_workers = mp.cpu_count(), proxies = [], level = 0):
    tweets_ids = set()

    if (input_filename.endswith('.csv')):
        from reader_csv_column import CsvFile,EXCLUDE
        csvfile = CsvFile(csvfilename)
        id_column = csvfile.get_column('id')
        for element in id_column:
            tweets_ids.add(int(element))
    elif (input_filename.endswith('.json')):
        with open(os.path.abspath(user_ids_config_filename), 'r') as user_ids_config_rf:
            tweets_ids = set(json.load(user_ids_config_rf))

    if (len(tweets_ids) > 0):
        return collect_retweets_by_tweets_ids(output_folder = output_folder, config = config, tweets_ids = tweets_ids, n_workers = n_workers, proxies = proxies, level = level)

def fetch_user_timeline_worker(user_config, now, output_folder, available, apikey_proxy_pairs_dict):

    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    apikeys = copy.copy(apikey_proxy_pairs_dict[available]['apikeys'])
    proxies = copy.copy(apikey_proxy_pairs_dict[available]['proxies'])

    proxies = iter(proxies) if proxies else None

    user_id = user_config['user_id']
    since_id = user_config['since_id'] if 'since_id' in user_config else 1

    logger.info('REQUEST -> (user_id: [%d]; since_id: [%d])'%(user_id, since_id))

    client_args = {"timeout": 30}

    retry = True
    remove = False
    try:
        while(retry):
            if proxies:
                proxy = next(proxies)
                logger.info('checking [%s]'%proxy)
                passed, proxy = check_proxy_twython(proxy['proxy'], 5)
                if not passed:
                    logger.warn('proxy failed, retry next one')
                    continue
                client_args['proxies'] = proxy['proxy_dict']

            twitterCralwer = TwitterCrawler(apikeys=apikeys, client_args=client_args, output_folder = output_folder)
            since_id, retry, remove = twitterCralwer.fetch_user_timeline(user_id, now=now, since_id = since_id)
            logger.info("since_id: %d; retry: %s"%(since_id, retry))
    # except StopIteration as exc:
    #     pass
    except Exception as exc:
        logger.error(exc)
        pass

    user_config['since_id'] = since_id
    user_config['remove'] = remove

    return available, user_config

def fetch_user_timeline_worker_done(future, now=None, output_folder = None, user_config_id = None, users_config = None, users_config_filename = None, available_apikey_proxy_pairs = []):

    available, user_config = future.result()

    users_config[user_config_id] = user_config

    with open(os.path.abspath(users_config_filename), 'w') as users_config_wf:
            json.dump(users_config, users_config_wf)

    day_output_folder = os.path.abspath('%s/%s'%(output_folder, now.strftime('%Y%m%d')))

    if not os.path.exists(day_output_folder):
        os.makedirs(day_output_folder)

    with open(os.path.abspath('%s/users.json'%day_output_folder), 'w') as users_config_wf:
        json.dump(users_config, users_config_wf)

    logger.info('finished... [%s]'%available)
    available_apikey_proxy_pairs.append(available)

def collect_tweets_by_user_ids(users_config_filename, output_folder, config, n_workers = mp.cpu_count(), proxies = []):

    apikey_proxy_pairs_dict = apikey_proxy_pairs(config['apikeys'], proxies)

    available_apikey_proxy_pairs = list(apikey_proxy_pairs_dict.keys())

    max_workers = len(available_apikey_proxy_pairs)

    #max_workers = mp.cpu_count() if max_workers > mp.cpu_count() else max_workers

    users_config = {}
    with open(os.path.abspath(users_config_filename), 'r') as users_config_rf:
        users_config = json.load(users_config_rf)

    max_workers = max_workers if max_workers < len(users_config) else len(users_config)
    max_workers = n_workers if n_workers < max_workers else max_workers
    logger.info("concurrent workers: [%d]"%(max_workers))

    futures_ = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:

        try:

            for user_config_id in itertools.cycle(users_config):
                user_config = users_config[user_config_id]
                if ('remove' in user_config and user_config['remove']):
                    continue

                while(len(available_apikey_proxy_pairs) == 0):
                    logger.info('no available_apikey_proxy_pairs, wait for %ds to retry...'%WAIT_TIME)
                    time.sleep(WAIT_TIME)

                now = datetime.datetime.now()
                future_ = executor.submit(
                            fetch_user_timeline_worker, user_config, now, output_folder, available_apikey_proxy_pairs.pop(), apikey_proxy_pairs_dict)

                future_.add_done_callback(functools.partial(fetch_user_timeline_worker_done, now=now, output_folder=output_folder, user_config_id = user_config_id, users_config=users_config, users_config_filename=users_config_filename, available_apikey_proxy_pairs=available_apikey_proxy_pairs))

                futures_.append(future_)
        except KeyboardInterrupt:
            logger.warn('You pressed Ctrl+C! But we will wait until all sub processes are finished...')
            concurrent.futures.wait(futures_)
            executor.shutdown()
            raise


def search_by_terms_worker(search_config, now, output_folder, available, apikey_proxy_pairs_dict):

    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    apikeys = apikey_proxy_pairs_dict[available]['apikeys']
    proxies = apikey_proxy_pairs_dict[available]['proxies']

    proxies = iter(proxies) if proxies else None

    search_terms = [term.lower() for term in search_config['terms']]
    querystring = '%s'%(' OR '.join('(' + term + ')' for term in search_terms))
    output_filename = search_config['output_filename'] if 'output_filename' in search_config else md5(querystring.encode('utf-8'))
    since_id = search_config['since_id'] if 'since_id' in search_config else 0


    logger.info('REQUEST -> (output_filename: [%s]; since_id: [%d];)'%(output_filename, since_id))

    client_args = {"timeout": 30}

    retry = True
    try:
        while(retry):
            if proxies:
                proxy = next(proxies)
                passed, proxy = check_proxy_twython(proxy['proxy'], 5)
                if not passed:
                    logger.warn('proxy failed, retry next one')
                    continue
                else:
                    logger.info('[%s] is alive'%proxy)
                client_args['proxies'] = proxy['proxy_dict']

            twitterCralwer = TwitterCrawler(apikeys=apikeys, client_args=client_args, output_folder = output_folder)
            since_id, retry = twitterCralwer.search_by_query(querystring, since_id = since_id, now=now, output_filename = output_filename)
            logger.info("since_id: %d; retry: %s"%(since_id, retry))
    # except StopIteration as exc:
    #     pass
    except Exception as exc:
        logger.error(exc)
        pass

    search_config['since_id'] = since_id
    search_config['querystring'] = querystring
    search_config['output_filename'] = output_filename

    #logger.info("return from: %s"%(search_config))
    return available, search_config


def search_by_terms_worker_done(future, output_folder=None, now = None, search_config_id = None, search_configs = None, search_configs_filename = None, available_apikey_proxy_pairs = []):

    logger.info("callback runs in PID: [%s]"%os.getpid())
    available, search_config = future.result()

    search_configs[search_config_id] = search_config

    with open(os.path.abspath(search_configs_filename), 'w') as search_configs_wf:
        json.dump(search_configs, search_configs_wf)

    day_output_folder = os.path.abspath('%s/%s'%(output_folder, now.strftime('%Y%m%d')))

    if not os.path.exists(day_output_folder):
        os.makedirs(day_output_folder)

    with open(os.path.abspath('%s/search.json'%day_output_folder), 'w') as search_configs_wf:
        json.dump(search_configs, search_configs_wf)

    logger.info('finished... [%s]'%available)
    available_apikey_proxy_pairs.append(available)


def collect_tweets_by_search_terms(search_configs_filename, output_folder, config, n_workers = mp.cpu_count(), proxies = []):

    logger.info("main runs in PID: [%s]"%os.getpid())

    apikey_proxy_pairs_dict = apikey_proxy_pairs(config['apikeys'], proxies)

    available_apikey_proxy_pairs = list(apikey_proxy_pairs_dict.keys())

    # at most the number of avaliable apikey and proxy pairs
    max_workers = len(available_apikey_proxy_pairs)

    # at most number of cpu
    #max_workers = mp.cpu_count() if max_workers > mp.cpu_count() else max_workers

    search_configs = {}
    with open(os.path.abspath(search_configs_filename), 'r') as search_configs_rf:
        search_configs = json.load(search_configs_rf)

    # at most number of searches
    max_workers = max_workers if max_workers < len(search_configs) else len(search_configs)
    max_workers = n_workers if n_workers < max_workers else max_workers

    logger.info("concurrent workers: [%d]"%(max_workers))

    futures_ = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:

        try:
            for search_config_id in itertools.cycle(search_configs):

                while(len(available_apikey_proxy_pairs) == 0):
                    logger.info('no available_apikey_proxy_pairs, wait for %ds to retry...'%WAIT_TIME)
                    time.sleep(WAIT_TIME)

                now = datetime.datetime.now()
                
                search_config = search_configs[search_config_id]

                future_ = executor.submit(
                            search_by_terms_worker, search_config, now, output_folder, available_apikey_proxy_pairs.pop(), apikey_proxy_pairs_dict)

                future_.add_done_callback(functools.partial(search_by_terms_worker_done, output_folder=output_folder, now = now, search_config_id = search_config_id, search_configs = search_configs, search_configs_filename = search_configs_filename, available_apikey_proxy_pairs=available_apikey_proxy_pairs))

                futures_.append(future_)
        except KeyboardInterrupt:
            logger.warn('You pressed Ctrl+C! But we will wait until all sub processes are finished...')
                # this is acutally impossible to run
            concurrent.futures.wait(futures_)

            executor.shutdown()
            raise
def search_by_city_worker_done(future, output_folder=None, now = None, search_configs_filename = None, available_apikey_proxy_pairs = []):

    available = future.result()


    available_apikey_proxy_pairs.append(available)

def search_by_city_worker(search_config, now, output_folder, available, apikey_proxy_pairs_dict):
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    apikeys = apikey_proxy_pairs_dict[available]['apikeys']
    proxies = apikey_proxy_pairs_dict[available]['proxies']
    logger.info("2 %s"%apikeys)
    logger.info("3 %s"%proxies)
    proxies = iter(proxies) if proxies else None
    logger.info('REQUEST -> (city: [%s];)'%(search_config))

    client_args = {"timeout": 60}

    retry = True
    try:
        while(retry):
            if proxies:
                proxy = next(proxies)
                passed, proxy = check_proxy_twython(proxy['proxy'], 5)
                if not passed:
                    logger.warn('proxy failed, retry next one')
                    continue
                else:
                    logger.info('[%s] is alive' %proxy)
                client_args['proxies'] = proxy['proxy_dict']

            twitterCralwer = TwitterCrawler(apikeys=apikeys, client_args=client_args, output_folder = output_folder, oauth2 = False)
            retry = twitterCralwer.fetch_geo(query = search_config, now = now)
            logger.info("retry: %s"%(retry))
    # except StopIteration as exc:
    #     pass
    except Exception as exc:
        logger.error(exc)
        pass

    return available

def collect_geo_info(cities_config_name = [], output_folder = None, config = None, n_workers = mp.cpu_count(), proxies = []):
    logger.info("main runs in PID: [%s]"%os.getpid())
    #Apikeys and proxy part
    apikey_proxy_pairs_dict = apikey_proxy_pairs(config['apikeys'], proxies)
    available_apikey_proxy_pairs = list(apikey_proxy_pairs_dict.keys())
    max_workers = len(available_apikey_proxy_pairs)

    citynames = cities_config_name

    logger.info("fetching [%d] cities"%(len(citynames)))

    max_workers = max_workers if max_workers < len(citynames) else len(citynames)
    max_workers = n_workers if n_workers < max_workers else max_workers
    logger.info("concurrent workers: [%d]"%(max_workers))

    futures_ = []

    with concurrent.futures.ProcessPoolExecutor(max_workers = max_workers) as executor:

        try:
            for city in citynames:

                while(len(available_apikey_proxy_pairs) == 0):
                    logger.info('no available_apikey_proxy_pairs, wait for %ds to retry...'%WAIT_TIME)
                    time.sleep(WAIT_TIME)

                now = datetime.datetime.now()

                logger.info("1 %s" %available_apikey_proxy_pairs)

                future_ = executor.submit(search_by_city_worker, city, now, output_folder, available_apikey_proxy_pairs.pop(), apikey_proxy_pairs_dict)

                future_.add_done_callback(functools.partial(search_by_city_worker_done, output_folder=output_folder, now = now, search_configs_filename = 'city_name.json', available_apikey_proxy_pairs=available_apikey_proxy_pairs))
#def search_by_city_worker_done(future, output_folder=None, now = None, search_config_id = None, search_configs = None, search_configs_filename = None, available_apikey_proxy_pairs = []):

                futures_.append(future_)
            else:
                concurrent.futures.wait(futures_)
                executor.shutdown()
                return False

        except KeyboardInterrupt:
            logger.warn('You pressed Ctrl+C! But we will wait until all sub processes are finished...')
            concurrent.futures.wait(futures_)
            executor.shutdown()
            raise

def collect_geo (input_filename, output_folder, config, n_workers = mp.cpu_count(), proxies = []):
    geolist = ["Coahuila Ocampo", "Coahuila Acuña", "Coahuila Piedras", "Coahuila Guerrero", "Coahuila Hidalgo", "Nuevo León Anáhuac", "Tamaulipas Nuevo Laredo", "Tamaulipas Guerrero", "Tamaulipas Mier", "Tamaulipas Miguel Alemán", "Tamaulipas Gustavo Díaz Ordaz", "Tamaulipas Reynosa", "Tamaulipas Río Bravo", "Tamaulipas Matamoros", "Texas El Paso", "Texas Hudspeth", "Texas Jeff Davis", "Texas Presidio County", "Texas Brewster County", "Texas Terrell County", "Texas Val Verde County", "Texas Kinney County", "Texas Maverick County", "Texas Webb County", "Texas Zapata County", "Texas Starr County", "Texas Hidalgo County", "Texas Cameron County", "California San Diego", "California Imperial", "Chihuahua Janos", "Chihuahua Ascensión", "Chihuahua Juárez", "Chihuahua Guadalupe", "Chihuahua Práxedis G. Guerrero", "Chihuahua Guadalupe", "Chihuahua Ojinaga", "Chihuahua Manuel Benavides", "Arizona Yuma", "Arizona Pima", "Arizona Santa Cruz", "Arizona Cochise", "New Mexico Hidalgo", "New Mexico Luna", "New Mexico Doña Ana", "Bajia California Tijuana", "Bajia California Tecate", "Bajia California Mxicali", "Sonora San Luis Río Colorado", "Sonora Puerto Peñasco", "Sonora Plutarco Elías Calles", "Sonora Caborca", "Sonora Altar", "Sonora Sáric", "Sonora Nogales", "Sonora Santa Cruz", "Sonora Cananea", "Sonora Naco", "Sonora Agua Prieta"]
    # if (input_filename.endswith('.csv')):
    #     pima = pd.read_csv(input_filename)
    #     pima.describe()
    #     citynames = pima['County'].tolist()
    # elif (input_filename.endswith('.json')):
    #     with open(os.path.abspath(input_filename), 'r') as citynames_rf:
    #         citynames = set(json.load(citynames_rf))
    #         print(citynames['geoname'])
    #         quit()

    # if (len(citynames) > 0):
    return collect_geo_info(cities_config_name = geolist, config = config, output_folder = output_folder, n_workers = n_workers, proxies = proxies)

    # with open(os.path.abspath(search_configs_filename), 'w') as search_configs_wf:
    #     json.dump(search_configs, search_configs_wf)

def fetch_history_done(future, now=None, tweets_config_filename=None, output_folder = None, tweets_config = None, available_apikey_proxy_pairs = []):

    available, tweet_config = future.result()

    tweets_config = tweet_config

    with open(os.path.abspath(tweets_config_filename), 'w') as tweets_config_wf:
            json.dump(tweets_config, tweets_config_wf)

    logger.info('finished... [%s]'%available)
    available_apikey_proxy_pairs.append(available)

def create_list(start = 0, end = 3061014649, endpoint = 0):

    if start + 100 > end:
        return list(range(start, end + 1)), end
    else:
        return list(range(start, start+100)), start + 100

def fetch_history_worker(tweets_id, now, end_point, output_folder, tweet_config, available, apikey_proxy_pairs_dict):

    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    apikeys = copy.copy(apikey_proxy_pairs_dict[available]['apikeys'])
    proxies = copy.copy(apikey_proxy_pairs_dict[available]['proxies'])

    proxies = iter(proxies) if proxies else None

    client_args = {"timeout": 30}

    retry = True
    try:
        while(retry):

            if proxies:
                proxy = next(proxies)
                passed, proxy = check_proxy_twython(proxy['proxy'], 5)
                if not passed:
                    logger.warn('proxy failed, retry next one')
                    continue
                else:
                    logger.info('[%s] is alive' %proxy)
                client_args['proxies'] = proxy['proxy_dict']
            twitterCralwer = TwitterCrawler(apikeys=apikeys, client_args=client_args, output_folder = output_folder)
            retry = twitterCralwer.lookup_history(tweets_id, now=now)

    # except StopIteration as exc:
    #     pass
    except Exception as exc:
        logger.error(exc)
        pass

    tweet_config["tweet_id"] = end_point

    return available, tweet_config

def collect_tweets_history(tweets_config_filename, output_folder, config, n_workers = mp.cpu_count(), proxies = []):

    apikey_proxy_pairs_dict = apikey_proxy_pairs(config['apikeys'], proxies)

    available_apikey_proxy_pairs = list(apikey_proxy_pairs_dict.keys())

    max_workers = len(available_apikey_proxy_pairs)

    #max_workers = mp.cpu_count() if max_workers > mp.cpu_count() else max_workers

    tweets_config = {}
    with open(os.path.abspath(tweets_config_filename), 'r') as tweets_config_rf:
        tweets_config = json.load(tweets_config_rf)

    max_workers = max_workers if max_workers < len(tweets_config) else len(tweets_config)
    max_workers = n_workers if n_workers < max_workers else max_workers
    logger.info("concurrent workers: [%d]"%(max_workers))

    futures_ = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        try:
            end_point = tweets_config['tweet_id']
            logger.info(type(tweets_config))
            while(end_point <= 3061014649):

                tweets_id, end_point = create_list(start = end_point)
                while(len(available_apikey_proxy_pairs) == 0):
                    logger.info('no available_apikey_proxy_pairs, wait for %ds to retry...'%WAIT_TIME)
                    time.sleep(WAIT_TIME)

                now = datetime.datetime.now()
                future_ = executor.submit(
                            fetch_history_worker, tweets_id, now, end_point, output_folder, tweets_config, available_apikey_proxy_pairs.pop(), apikey_proxy_pairs_dict)

                future_.add_done_callback(functools.partial(fetch_history_done, now=now, output_folder=output_folder, tweets_config_filename = tweets_config_filename, tweets_config = tweets_config, available_apikey_proxy_pairs=available_apikey_proxy_pairs))

                futures_.append(future_)
        except KeyboardInterrupt:
            logger.warn('You pressed Ctrl+C! But we will wait until all sub processes are finished...')
            concurrent.futures.wait(futures_)
            executor.shutdown()
            raise


if __name__=="__main__":
    formatter = logging.Formatter('(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
    handler = logging.handlers.RotatingFileHandler(
        'twitter_tracker.log', maxBytes=50 * 1024 * 1024, backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="config.json that contains twitter api keys;", default="./config.json")
    parser.add_argument('-p', '--proxies', help="the proxies.json file", default="./proxies.json")
    parser.add_argument('-o','--output', help="output folder data", default="./data/")
    parser.add_argument('-cmd','--command', help="search by keywords (search) or crawl user timelines (timeline)", default="search")
    parser.add_argument('-cc','--command_config', help="existing progress data", default="search.json")
    parser.add_argument('-l','--level', help = "typing a int to indicate how many layer of retweets you want to fetch", type = int, default = 3)
    parser.add_argument('-w','--workers', help="number of workers (will only be effective if it's smaller than the number of proxies avaliable)", type=int, default=8)
    parser.add_argument('-wait','--wait_time', help="wait time to check available api keys", type=int, default=30)

    args = parser.parse_args()

    if not args.command:
        raise MissingArgs('command is missing')

    WAIT_TIME = args.wait_time

    with open(os.path.abspath(args.config), 'r') as config_f:
        config = json.load(config_f)

        try:

            proxies = []
            if args.proxies:
                with open(os.path.abspath(args.proxies), 'r') as proxy_f:
                    proxies = proxy_checker(json.load(proxy_f))
                    logger.info("there are [%d] live proxies"%len(proxies))

            retry = True

            while (retry):
                try:
                    if (args.command == 'search'):
                        collect_tweets_by_search_terms(args.command_config, args.output, config, args.workers, proxies)
                    elif (args.command == 'timeline'):
                        collect_tweets_by_user_ids(args.command_config, args.output, config, args.workers, proxies)
                    elif (args.command == 'users_by_user_id'):
                        retry = collect_users('user_id', args.command_config, args.output, config, args.workers, proxies)
                    elif (args.command == 'users_by_screen_name'):
                        retry = collect_users('screen_name', args.command_config, args.output, config, args.workers, proxies)
                    elif (args.command in ['/friends/ids', '/friends/list', '/followers/ids', '/followers/list']):
                        retry = collect_user_relatinoships_by_user_ids(args.command, args.command_config, args.output, config, args.workers, proxies)
                    elif (args.command == '/statuses/retweets/:id'):
                        retry = collect_retweets(args.command_config, args.output, config, args.workers, proxies, args.level)
                    elif (args.command == 'getgeo'):
                        retry = collect_geo(args.command_config, args.output, config, args.workers, proxies)
                    elif (args.command == 'history'):
                        retry = collect_tweets_history(args.command_config, args.output, config, args.workers, proxies)
                except KeyboardInterrupt:
                    retry = False
                    raise
                except concurrent.futures.process.BrokenProcessPool:
                    retry = True
                except Exception as exc:
                    logger.error(exc)
                    logger.error(full_stack())
                    retry = True

        except KeyboardInterrupt:
            logger.error('Ok, killed myself...')
            pass
        except Exception as exc:
            logger.error(exc)
            logger.error(full_stack())
        finally:
            pass
