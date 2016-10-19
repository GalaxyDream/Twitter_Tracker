#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

import sys
import json
import math

def explode(coords):
    """Explode a GeoJSON geometry's coordinates object and yield coordinate tuples.
    As long as the input is conforming, the type of the geometry doesn't matter."""
    for e in coords:
        if isinstance(e, (float, int, int)):
            yield coords
            break
        else:
            for f in explode(e):
                yield f

def bbox(f):
    x, y = zip(*list(explode(f['geometry']['coordinates'])))
    return min(x), min(y), max(x), max(y)

def to_bbox_polygon(bounding_box):

    min_x, min_y, max_x, max_y = bounding_box

    polygon = {
        'type': 'Polygon',
        'coordinates': [[
            [min_x, min_y],
            [min_x, max_y],
            [max_x, max_y],
            [max_x, min_y],
            [min_x, min_y]
        ]]
    }

    return polygon

MAX = 25
def process_us_states(geoJSON_file):
    with open(geoJSON_file) as gf:
        us = json.load(gf)
        logger.info(len(us['features']))
        name = []
        locations = []
        cnt = 0
        for state in us['features']:
            #logger.info(state['properties'])
            #min_x, min_y, max_x, max_y = bbox(state)

            name.append(state['properties']['NAME'])
            locations.append(','.join(['%s'%(x) for x in list(bbox(state))]))
            cnt += 1

            if (cnt % MAX == 0):
                n = 'US_BY_STATE_%d.json'%(math.ceil(cnt/MAX))
                with open(n, 'w') as wf:
                    json.dump({'name': n, 'locations': ','.join(locations)}, wf)
                
                name = []
                locations = []
            #logger.info(locations)
            #logger.info(json.dumps(to_bbox_polygon(bbox(state))))
        else:
            if (len(name) > 0):
                n = 'US_BY_STATE_%d.json'%(math.ceil(cnt/MAX))
                with open(n, 'w') as wf:
                    json.dump({'name': n, 'locations': ','.join(locations)}, wf)


def process_us_counties(geoJSON_file):
    with open(geoJSON_file, 'rb') as gf:
        us = json.loads(gf.read().decode('utf-8','ignore'))
        logger.info(len(us['features']))
        
        name = []
        locations = []
        cnt = 0
        
        for county in us['features']:
            logger.info(county)
            quit()

if __name__=="__main__":

    logger.info(sys.version)

    #process_us_states('gz_2010_us_040_00_20m.json')
    process_us_counties('gz_2010_us_050_00_20m.json')

