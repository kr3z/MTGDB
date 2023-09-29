import requests
import time
import logging

logger = logging.getLogger('MTG.scryfall')

SCRYFALL_API = "https://api.scryfall.com/"
last_req = 0
scryfall_session = None

class ScryfallRequestException(Exception):
    pass

def scryfall_api_request(api,p=None,limit=None):
    return(scryfall_request(SCRYFALL_API+api,p,limit))

def scryfall_request(url,p=None, limit=None):
    global last_req, scryfall_session
    if scryfall_session is None:
        scryfall_session = requests.Session()
    time_since_last_req = round(time.time() * 1000) - last_req 
    if time_since_last_req < 100:
        sleep_time = (100 - time_since_last_req)/1000.0
        logger.debug("Sleeping for API rate limit: %d ms" ,sleep_time*1000)
        time.sleep(sleep_time)
    try:
        response = scryfall_session.get(url,params=p,stream=False)
    except Exception as e:
        logger.error(e)
        scryfall_session.close()
        time.sleep(.1)
        scryfall_session = requests.Session()
        response = scryfall_session.get(url,params=p,stream=False)
    last_req = round(time.time() * 1000)
    if response.status_code != 200:
        raise ScryfallRequestException(response)
    
    r = response.json()

    data = r.get("data")

    if "warnings" in r.keys():
        #TODO handle warnings
        warnings = r.get("warnings")
        logger.warn("Scryfall request returned warnings: %s", warnings)
    has_more = r.get("has_more")
    if has_more:
        next_page = r.get("next_page")
    if "total_cards" in r.keys():
        total_cards = r.get("total_cards")
        logger.info("Retrieved batch of %d out of %d total results" ,len(data), total_cards)
    else:
        logger.info("Retrieved %d results", len(data))

    if has_more and (limit is None or len(data)<limit):
        logger.debug("More data exists, retrieving next batch")
        new_limit = None if limit is None else limit-len(data)
        data.extend(scryfall_request(next_page,limit=new_limit))

    if limit is not None and len(data)>limit:
        data = data[:limit]
    return data