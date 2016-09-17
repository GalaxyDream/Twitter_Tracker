#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)
from util import full_stack
import requests, concurrent.futures, os, json
import twython

def check_proxy_twython(proxy, timeout):
    #APP_KEY = "TMsmNlRUDt3HckXGMduLrqKPz"
    #APP_SCRET = "dUk0pYMioi3oHvjIFXzfN5DPowGIy04A63FtoNH9zYePR14QOo"
    APP_KEY = "Zt9Zfm2dmvx3PWc5o9VS3AEkz"
    APP_SCRET = "Ljp8bDrpaOJJobxwjx8i25IjzJEoBIDKg92VCup3F4Dx8FR76L"

    proxy_ip = list(proxy.keys())[0]
    proxy_type = list(proxy.values())[0]

    p = {
        'proxy':proxy,
        'proxy_dict':{
            "http": '%s://%s'%(proxy_type, proxy_ip),
            "https:": '%s://%s'%(proxy_type, proxy_ip)
        }
    }


    client_args = {
        "timeout": timeout,
        'proxies': {
            'http': '%s://%s'%(proxy_type, proxy_ip),
            'https': '%s://%s'%(proxy_type, proxy_ip)
        }
    }

    twitter = twython.Twython(APP_KEY, APP_SCRET, oauth_version=2, client_args=client_args)

    try:
        ACCESS_TOKEN = twitter.obtain_access_token()
        twitter = twython.Twython(APP_KEY, access_token=ACCESS_TOKEN, client_args=client_args)

        #twitter.search(q='python')
        if (ACCESS_TOKEN):
            return True, p
        else:
            return False, None
    except Exception as exc:
        logger.info("proxy [%s] failed: %s"%(p['proxy'], exc))
        return False, None

# for whatever reason https proxy isn't working... 
def check_proxy(proxy, timeout):
    url = "http://twitter.com"
    # headers = {
    #     'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:23.0) Gecko/20100101 Firefox/23.0',
    #     'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    #     'Accept-Encoding': 'gzip, deflate',
    #     'Accept-Language': 'en-US,en;q=0.5'
    # }
    proxy_ip = list(proxy.keys())[0]
    proxy_type = list(proxy.values())[0]

    p = {
        'proxy':proxy,
        'proxy_dict':{
            "http": '%s://%s'%(proxy_type, proxy_ip),
            "https:": "%s://%s"%(proxy_type, proxy_ip)
        }
    }

    proxies = p['proxy_dict']
    #logger.info(p['proxy_dict'])
    try:
        #s = requests.Session()
        #s.proxies = p['proxy_dict']
        r = requests.get(url, proxies=proxies, verify=True)
        #r = s.get(url)

        if (r.status_code == requests.codes.ok):
            return True, p
        else:
            return False, None
    except Exception as exc:
        logger.info("proxy [%s] failed: %s"%(p['proxy'], exc))
        return False, None

def proxy_checker(proxies):
    '''
        proxies is a list of {key:value}, where the key is the ip of the proxy (including port), e.g., 192.168.1.1:8080, and the value is the type of the proxy (http/https)
    '''

    logger.info('%d proxies to check'%(len(proxies)))
    import multiprocessing as mp
    

    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=mp.cpu_count()*10) as executor:

        future_to_proxy = {executor.submit(check_proxy_twython, proxy, 5): proxy for proxy in proxies if list(proxy.values())[0] == 'http'}

        for future in future_to_proxy:
            future.add_done_callback(lambda f: results.append(f.result()))
            
        logger.info('%d http proxies to check'%(len(future_to_proxy)))

        concurrent.futures.wait(future_to_proxy)

        # for future in futures.as_completed(future_to_proxy):

        #   proxy = future_to_proxy[future]
        #   try:
        #       good, proxy_dict = future.result()
        #   except Exception as exc:
        #       logger.info('%r generated an exception: %s'%(proxy, exc))
        #   else:
        #       if (good):
        #           good_proxies.append(proxy_dict)

        executor.shutdown()
        
        return [p for (good, p) in results if good]


def check_hma_proxies():

    filename = os.path.abspath('./proxies/01-13-2015/full_list/_full_list.txt')
    proxies_to_be_checked = []
    logger.info(filename)
    with open(filename, 'r') as rf:
        for line in rf:
            ip = line.strip()
            proxies_to_be_checked.append({ip:'http'})

    proxies = []
    for p in proxy_checker(proxies_to_be_checked):
        proxies.append(p['proxy'])

    with open('proxies.json', 'w') as wf:
        json.dump(proxies, wf)

def check_proxies():
    proxies = []
    with open('proxies.json', 'r') as rf:
        proxies_to_be_checked = json.load(rf)

        for p in proxy_checker(proxies_to_be_checked):
            proxies.append(p['proxy'])

        with open('proxies.json', 'w') as wf:
            json.dump(proxies, wf)

if __name__=="__main__":

    #check_hma_proxies()
    #check_proxy({"54.84.61.133:8888":"http"}, 5)
    #check_proxy_twython({'213.85.92.10:80':'http'})
    check_proxies()




