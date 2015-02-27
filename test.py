#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.DEBUG)

import sys, time, argparse, json, os, pprint
import twython
from util import full_stack, chunks, md5
from proxy_check import check_proxy, proxy_checker
from exceptions import NotImplemented, MissingArgs, WrongArgs, InvalidConfig, MaxRetryReached
import concurrent.futures
import functools
sys.path.append(".")


from twitter_tracker import TwitterCrawler


def test_twython_proxy():
    apikeys = {
        "app_key":"m1laIIM5JEJt7O9eEzUg",
        "app_secret":"GMh2YtW3pBijwMcDLS1LtR5bElVd3SOeytyG5agE",
        "oauth_token":"1948122342-EpbQDurZgv5bfZsTtJio9t9gUw7a2k0FHPl87cj",
        "oauth_token_secret":"Eyj60DhrtCF1KgJZ1ZvXvsSEHxIQVB6EYnTIu6RKEe8"
    }

    proxies = [{"108.165.33.8:3128": "http"}, {"107.182.17.243:8089": "http"}]
    proxies = proxy_checker(proxies)

    search_terms = ["#blacktransrevolution"]
    search_terms = [term.lower() for term in search_terms]
    querystring = '%s'%(' OR '.join('"' + term + '"' for term in search_terms))


    for proxy in proxies:
        passed, proxy = check_proxy(proxy['proxy'], 5)

        if not passed:
           logger.warn('proxy failed, retry next one')
           continue

        client_args = {
            "timeout": 5,
            "proxies": proxy['proxy_dict']
        }
        # {
        #         "http": "http://108.165.33.8:3128",
        #         "https": "http://108.165.33.8:3128"
        #     }
        logger.info('using [%s]'%proxy)

        max_id = 0
        twitterCralwer = TwitterCrawler(apikeys=apikeys, client_args=client_args, output_folder = './data')
        max_id, retry = twitterCralwer.search_by_query(querystring, current_max_id = max_id, output_filename = 'testing')

        quit()


def test_proxy():

    with open(os.path.abspath('proxies.json'), 'r') as proxy_f:
        proxies = []
        for p in proxy_checker(json.load(proxy_f)):
            proxies.append(p['proxy'])

        with open('proxies.json', 'w') as wf:
            json.dump(proxies, wf)


if __name__=="__main__":

    test_proxy()
