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





def generate_search_json(query_terms, search_json_filename):

    with open(search_json_filename, 'w') as wf:
        results = {}

        #querystring = '%s'%(', '.join('"' + term.lower() + '"' for term in query_terms))
        querystring = ''

        current_query_terms = []

        for term in query_terms:
            querystring = '%s OR %s'%(querystring, term.lower())
            current_query_terms.append(term.lower())

            if (len(querystring) > 200):

                output_filename = md5(querystring.encode('utf-8'))

                logger.info(current_query_terms)
                
                results[output_filename] = {
                    "terms": current_query_terms,
                    "since_id": 0,
                    "querystring": querystring,
                    "output_filename": output_filename
                }

                current_query_terms = []
                querystring = ''


        json.dump(results, wf)


if __name__=="__main__":

    search_terms = ["hpv", "#hpv", "Human Papillomavirus Vaccines", "Human Papillomavirus Vaccine",
    "HumanPapillomavirusVaccine", "#HumanPapillomavirusVaccine",
    "Gardasil", "#Gardasil", "Human Papillomavirus", "Cervarix",
    "cervical cancer", "cervical #cancer", "#cervical cancer", "cervicalcancer", "#cervicalcancer"
    ]

    query_terms = ['"%s"'%(term) for term in search_terms]

    generate_search_json(query_terms, "hpv_search.json")