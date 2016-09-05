#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import logging.handlers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

import sys,json
import hashlib
def md5(data):
    return hashlib.md5(data).hexdigest()


search_terms = ["hpv", "#hpv", "Human Papillomavirus Vaccines", "Human Papillomavirus Vaccine",
    "HumanPapillomavirusVaccine", "#HumanPapillomavirusVaccine",
    "Gardasil", "#Gardasil", "Human Papillomavirus", "Cervarix",
    "cervical cancer", "cervical #cancer", "#cervical cancer", "cervicalcancer", "#cervicalcancer"
    ]
def generate_search_json():

    with open('search.json', 'w') as wf:
        results = {}

        querystring = '%s'%(', '.join('"' + term.lower() + '"' for term in search_terms))

        logger.info(querystring)
        #quit()
        output_filename = md5(querystring.encode('utf-8'))

        
        results[output_filename] = {
            "terms": search_terms,
            "since_id": 0,
            "querystring": querystring,
            "output_filename": output_filename
        }

        json.dump(results, wf)

if __name__=="__main__":

    generate_search_json()