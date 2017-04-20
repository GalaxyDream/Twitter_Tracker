#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: ji0ng.bi0n

import logging
import logging.handlers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')

import sys, json, os, argparse

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def split_user_ids(input_file, n=10000):

    user_ids = []
    with open(os.path.abspath(input_file), 'r') as user_ids_config_rf:
        user_ids = json.load(user_ids_config_rf)

    user_ids = list(set(user_ids))

    for n, chunk in enumerate(chunks(user_ids, n)):
        with open('%s_%d.json'%(input_file.strip('.json'), n), 'w') as of:
            json.dump(chunk, of)


if __name__=="__main__":

    logger.info(sys.version)
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help="input file of the user_ids.json", default="user_ids.json")
    parser.add_argument('-n', '--number', help="the size of each chunk", type=int, default=10000)
    
    args = parser.parse_args()

    split_user_ids(args.input, n=args.number)

