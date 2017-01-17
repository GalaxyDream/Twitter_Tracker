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

def generate_search_json(query_terms, geocodes, search_json_filename):

    with open(search_json_filename, 'w') as wf:
        results = {}

        #querystring = '%s'%(', '.join('"' + term.lower() + '"' for term in query_terms))
        querystring = ''

        current_query_terms = []

        if (not geocodes):
            geocodes = [None]

        for geocode in geocodes:
            #logger.info(geocode)
            for term in query_terms:
                querystring = '%s OR %s'%(querystring, term.lower())
                current_query_terms.append(term.lower())

                if (len(querystring) > 200):

                    output_filename = md5(('%s_%s'%(geocode,querystring)).encode('utf-8'))

                    #logger.info(current_query_terms)
                    
                    results[output_filename] = {
                        "terms": current_query_terms,
                        "since_id": 0,
                        "geocode": geocode,
                        "querystring": querystring,
                        "output_filename": output_filename
                    }

                    current_query_terms = []
                    querystring = ''

            if (len(current_query_terms) > 0):
                output_filename = md5(('%s_%s'%(geocode,querystring)).encode('utf-8'))

                #logger.info(current_query_terms)
                
                results[output_filename] = {
                    "terms": current_query_terms,
                    "since_id": 0,
                    "geocode": geocode,
                    "querystring": querystring,
                    "output_filename": output_filename
                }

        json.dump(results, wf)


if __name__=="__main__":

    # search_terms = ["hpv", "#hpv", "#hpvvaccine", "hpvvaccine", "HumanPapillomavirus", "#HumanPapillomavirus",
    # "HumanPapillomavirusVaccine", "#HumanPapillomavirusVaccine",
    # "Gardasil", "#Gardasil", "Papillomavirus", "#Papillomavirus", "Cervarix", "#cervarix"
    # "cervical cancer", "cervical #cancer", "#cervical cancer", "cervicalcancer", "#cervicalcancer",
    #     "vph",
    #     "#vph",
    #     "#vphvacuna",
    #     "vphvacuna",
    #     "viruspapilomahumano",
    #     "#viruspapilomahumano",
    #     "viruspapilomahumanovacuna",
    #     "#viruspapilomahumanovacuna",
    #     "viruspapiloma",
    #     "#viruspapiloma",
    #     "cancer cervical",
    #     "cancer #cervical",
    #     "cancercervical",
    #     "#cancercervical"
    # ]

    # search_terms = [
    #     "vph",
    #     "#vph",
    #     "#vphvacuna",
    #     "vphvacuna",
    #     "viruspapilomahumano",
    #     "#viruspapilomahumano",
    #     "viruspapilomahumanovacuna",
    #     "#viruspapilomahumanovacuna",
    #     "Gardasil",
    #     "#Gardasil",
    #     "viruspapiloma",
    #     "#viruspapiloma",
    #     "Cervarix",
    #     "#Cervarix",
    #     "cancer cervical",
    #     "cancer #cervical",
    #     "#cervical cancer",
    #     "cancercervical",
    #     "#cancercervical"
    # ]
    search_terms = [
      "lynch syndrome",
      "#lynchsyndrome",
      "lynchsyndrome",
      "#lynch_syndrome"
    ]
    query_terms = ['"%s"'%(term) for term in search_terms]

    geocodes = None
    # geocodes = [
    #     ("san_diego_ca","33.02187484500974,-116.84265566973069,55.24883155189012mi"),
    #     ("imperial_ca","33.02863311995613,-115.28832454648432,55.353165830561416mi"),
    #     ("el_paso_tx","31.69479577023928,-106.31377858272373,28.188316540742175mi"),
    #     ("hudspeth_tx","31.31421173053295,-105.45678768595083,57.40059197269535mi"),
    #     ("jeffdavis_tx","30.760986418542448,-104.21545181087042,51.796749098745266mi"),
    #     ("presidio_tx","29.947431275579614,-104.39313153430696,59.107777736166476mi"),
    #     ("brewster_tx","29.82108021975194,-103.06701898626915,73.33286325065248mi"),
    #     ("terrell_tx","30.217388953656553,-102.10870342309578,40.92253295162357mi"),
    #     ("val_verde_tx","29.766272774689806,-101.23326432293436,48.14071357592185mi"),
    #     ("kinney_tx","29.354367391377842,-100.45343798847033,27.737542948833962mi"),
    #     ("maverick_tx","28.6418766839092,-100.39106155970319,34.98876630916757mi"),
    #     ("webb_tx","27.736440149847414,-99.50681922640004,54.005019300303424mi"),
    #     ("zapata_tx","26.945237068429957,-99.20491891311715,30.021569916528225mi"),
    #     ("starr_tx","26.518652788944046,-98.74735635911306,32.16693558200124mi"),
    #     ("hidalgo_tx","26.413870239544636,-98.22543255064708,33.948510255991465mi"),
    #     ("cameron_tx","26.13433662091788,-97.50516378236739,29.35485722420209mi"),
    #     ("hidalgo_nm","32.05563012865423,-108.63254286591285,55.57785334065308mi"),
    #     ("luna_nm","32.195421724199115,-107.76514700356664,39.34474044833963mi"),
    #     ("dona_ana_nm","32.41906196127472,-106.82334114385034,51.93959956432837mi"),
    #     ("yuma_az","32.75376341318896,-114.07933889710172,65.36135351845566mi"),
    #     ("pima_az","31.977987667592142,-111.9011017576389,92.57723137311442mi"),
    #     ("santa_cruz_az","31.53272752269616,-110.91016210178388,30.28789444140191mi"),
    #     ("cochise_az","31.88187941076444,-109.75824543276532,56.08862590170022mi")
    # ]

    generate_search_json(query_terms, geocodes, "lynchsyndrome_search.json")