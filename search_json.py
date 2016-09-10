#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import logging.handlers
#import re

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

import sys,json
import hashlib
def md5(data):
    return hashlib.md5(data).hexdigest()
# n is the number in one list element
def chunks(arr, n):
    return [arr[i:i+n] for i in range(0, len(arr), n)]

search_terms_list= ['Adenoma', '#Adenoma', 'Colorectal', '#Colorectal','Colon', '#Colon','Carcinoid tumours','Carcinoidtumours','#Carcinoid tumours','Double contrast barium enema','Doublecontrastbariumenema','#Double contrast barium enema','Lymphomas','#Lymphomas','Rectum','#Rectum','Rectal bleeding','#Rectal bleeding','Polyp','#Polyp','FAP','#FAP','HNPCC','#HNPCC','TNM system','#TNM system','High sensitivity fecal occult blood tests','#High sensitivity fecal occult blood tests','Stool DNA test','#Stool DNA test','Sigmoidoscopy','#Sigmoidoscopy','Squamous','#Squamous','Sarcomas','#Sarcomas']



<<<<<<< HEAD
def generate_search_json():
=======
>>>>>>> e71b428031cbde0f6ed2077de07e9442cbdfc2cc



def generate_search_json(query_terms, search_json_filename):

    with open(search_json_filename, 'w') as wf:
        results = {}
<<<<<<< HEAD
        #for term in search_terms:
            #choke = re.findall(r'. {len(term)}',term)
        # collection_query = []
        # choke_length(400, len(search_terms) - 1, '',collection_query, 0)
        search_terms_collect = chunks(search_terms_list, 4)
        print(search_terms_collect)
        for search_terms in search_terms_collect:
            querystring = ''
            for term in search_terms:
                querystring += 'OR' + '"' + term.lower() + '"'
            #print(collection)
            print(len(querystring))
            output_filename = md5(querystring.encode('utf-8'))
            results[output_filename] = {
                       "terms": search_terms,
                       "since_id": 0,
                       "querystring": querystring[2:],
                       "output_filename": output_filename
                }
=======

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

>>>>>>> e71b428031cbde0f6ed2077de07e9442cbdfc2cc

        json.dump(results, wf)
# def choke_length(distance, stop_judgement, str_query, collection, cnt):
#     #stop and add judgement
#     if(stop_judgement <= 0):
#         #
#         collection.append(str_query[cnt:])
#         print (collection)
#         return
#     if(len(str_query) >= distance):
        
#         collection.append(str_query[3:])
#         #choke_length(distance, stop_judgement,'', collection, cnt)
#     #recersive
#     str_query +='OR'+'"' + search_terms[cnt].lower() +'"' 
#     cnt += 1
#     choke_length(distance, stop_judgement - 1, str_query, collection, cnt)
    # while (cnt <= len(search_terms)):
    #     str_query =''
    #     for term in search_terms[cnt:]:
    #             if (len(str_query) < distance):
    #                 str_query +='OR'+'"' + term +'"' 
    #                 cnt+=1
    #             else:
    #                 collection.append(str_query)
    #                 break
    # print(collection)
    


if __name__=="__main__":

    search_terms = ["hpv", "#hpv", "Human Papillomavirus Vaccines", "Human Papillomavirus Vaccine",
    "HumanPapillomavirusVaccine", "#HumanPapillomavirusVaccine",
    "Gardasil", "#Gardasil", "Human Papillomavirus", "Cervarix",
    "cervical cancer", "cervical #cancer", "#cervical cancer", "cervicalcancer", "#cervicalcancer"
    ]

    query_terms = ['"%s"'%(term) for term in search_terms]

    generate_search_json(query_terms, "hpv_search.json")