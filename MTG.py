import json
import re
import os
import argparse
import traceback
import logging
import logging.config
from datetime import datetime
from DB import DBConnection
from Scryfall import scryfall_api_request, scryfall_request
from MTGClasses import MTGPrint, MTGCard, MTGSet, MTGPrice, CardFace, RelatedCard, Legalities

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('MTG')

BATCH_SIZE= 1000

#TODO: desc
desc = """
Maintains a Database of 'Magic: The Gathering' cards and prices.
Data can be populated using Scryfall's REST API or by importing a Scryfall bulk data file
Collection data can be imported from Card Castle export files
"""

parser = argparse.ArgumentParser(description=desc)
parser.add_argument('action', choices=['import_files','update_sets','update_cards'])
args = parser.parse_args()

def import_card_data(data, data_date = datetime.now(),conn=None):    
    for p in data:
        prnt = MTGPrint(p,data_date = data_date)
        card = MTGCard(p,data_date = data_date)

    # TODO: Log counts of parsed objects
    #logger.info("New Prints: %d New Cards: %d" ,len(new_prints),len(new_cards))
    #logger.info("Updated Prints: %d Updated Cards: %d" ,len(update_prints),len(update_cards))

    counts = {'NewCards': 0, 'UpdatedCards': 0, 'NewPrints': 0, 'UpdatedPrints': 0, 'NewPrices': 0}

    close_conn = conn is None
    cursor = None
    try:
        if conn is None:
            conn = DBConnection()
        cursor = conn.getCursor()

        while(MTGCard.hasNewData()):
            new_card_data = MTGCard.getNewBatch()
            cursor.executemany(MTGCard.insert_sql,new_card_data)

            conn.commit()
            counts['NewCards']+=len(new_card_data)
            logger.info("Cards Imported: %d" ,counts['NewCards'])

        while(MTGCard.hasUpdateData()):
            update_card_data = MTGCard.getUpdateBatch()
            cursor.executemany(MTGCard.update_sql,update_card_data)
            conn.commit()
            counts['UpdatedCards']+=len(update_card_data)
            logger.info("Cards Updated: %d" ,counts['UpdatedCards'])


        # TODO: COnsilidate new and update blocks
        while(MTGPrint.hasNewData()):
            new_print_data, new_addl_print_data, new_addl_data = MTGPrint.getNewBatch()
            cursor.executemany(MTGPrint.insert_sql,new_print_data)
            cursor.executemany(MTGPrint.insert_addl_sql,new_addl_print_data)

            print_keys = [row[-1] for row in new_print_data]
            
            cursor.executemany(CardFace.insert_sql,CardFace.getBatchData())
            cursor.executemany(RelatedCard.insert_sql,RelatedCard.getBatchData())
            cursor.executemany(Legalities.insert_sql,Legalities.getBatchData())

            addl_data = []
            # TODO: This can be improved in getBatch since we already have print_key
            for print_key in print_keys:
                if new_addl_data.get(print_key):
                    for data in new_addl_data.get(print_key):
                        addl_data.append([print_key] + data)

            cursor.executemany(MTGPrint.insert_addl_data_sql,addl_data)

            conn.commit()
            counts['NewPrints']+=len(new_print_data)
            logger.info("Prints Imported: %d" ,counts['NewPrints'])

        while(MTGPrint.hasUpdateData()):
            # TODO: update legalities instead of delete/re-insert
            update_print_data, update_addl_data = MTGPrint.getUpdateBatch()
            update_print_data, update_print_addl_data, update_addl_data = MTGPrint.getUpdateBatch()
            cursor.executemany(MTGPrint.update_sql,update_print_data)
            cursor.executemany(MTGPrint.update_addl_sql,update_print_addl_data)

            print_keys = [row[-1] for row in update_print_data]

            card_face_delete_sql = CardFace.delete_sql_start + ','.join(["%s"]*len(print_keys)) + ")"
            related_card_delete_sql = RelatedCard.delete_sql_start + ','.join(["%s"]*len(print_keys)) + ")"
            legalities_delete_sql = Legalities.delete_sql_start + ','.join(["%s"]*len(print_keys)) + ")"
            addl_data_delete_sql = MTGPrint.addl_data_delete_sql_start + ','.join(["%s"]*len(print_keys)) + ")"
            cursor.execute(card_face_delete_sql,print_keys)
            cursor.execute(related_card_delete_sql,print_keys)
            cursor.execute(legalities_delete_sql,print_keys)
            cursor.execute(addl_data_delete_sql,print_keys)

            cursor.executemany(CardFace.insert_sql,CardFace.getBatchData())
            cursor.executemany(RelatedCard.insert_sql,RelatedCard.getBatchData())
            cursor.executemany(Legalities.insert_sql,Legalities.getBatchData())
            # cursor.executemany(Legalities.update_sql,Legalities.getBatchData())

            addl_data = []
            # TODO: This can be improved in getBatch since we already have print_key
            for print_key in print_keys:
                if update_addl_data.get(print_key):
                    for data in update_addl_data.get(print_key):
                        addl_data.append([print_key] + data)
            cursor.executemany(MTGPrint.insert_addl_data_sql,addl_data)
            #cursor.executemany(MTGPrint.legalities_update_sql,legalities_data)

            conn.commit()
            counts['UpdatedPrints']+=len(update_print_data)
            logger.info("Prints Updated: %d" ,counts['UpdatedPrints'])

        # Update prices
        while(MTGPrice.hasData()):
            price_data = MTGPrice.getBatch()
            cursor.executemany(MTGPrice.insert_sql,price_data)
            conn.commit()
            counts['NewPrices']+=len(price_data)
            logger.info("Prices Added: %d" ,counts['NewPrices'])

    except Exception as error:
        print(error)
        traceback.print_exc()
        if conn is not None:
            conn.rollback()
        raise(error)
    finally:
        if conn is not None and close_conn:
            conn.close()

    return counts


def update_sets():
    set_data = scryfall_api_request('sets')
    sets = []
    for s in set_data:
        sets.append(MTGSet(s))
    logger.info("Total sets retrieved from scryfall: %d" ,len(sets))

    conn = None
    cursor = None
    try:
        conn = DBConnection()
        cursor = conn.getCursor()
                
        while(MTGSet.hasNewData()):
            new_set_data = MTGSet.getNewBatch()
            cursor.executemany(MTGSet.insert_sql,new_set_data)
            
            conn.commit()
            logger.info("Sets Imported: %d" ,len(new_set_data))

        while(MTGSet.hasUpdateData()):
            update_set_data = MTGSet.getUpdateBatch()
            cursor.executemany(MTGSet.update_sql,update_set_data)
            conn.commit()
            logger.info("Sets Updated: %d" ,len(update_set_data))

    except Exception as error:
        print(error)
        traceback.print_exc()
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()
    
def update_cards_by_set(scryfall_id):
    ret = None
    res = DBConnection.singleQuery("SELECT search_uri FROM Sets where scryfall_id = %s",[scryfall_id])
    if res is not None:
        search_uri = res[0][0]
        search_uri += "&include_multilingual=true"
        logger.debug("Retrieving cards for Set scryfall_id: %s using URI: %s", scryfall_id, search_uri)
        set_data = scryfall_request(search_uri)
        if set_data is not None and len(set_data)>0:
            logger.debug("Retrieved %d cards from URI: %s", len(set_data),search_uri)
            ret = import_card_data(set_data)
        else:
            logger.error("No data returned from Set search URI: %s", search_uri)
    else:
        logger.error("No Set found for scryfall id: %s",scryfall_id)

    return ret

def update_all_cards_and_sets():
    total_cards_added = 0
    total_cards_updated = 0
    total_prints_added = 0
    total_prints_updated = 0
    total_prices_added = 0
    logger.info("Performing full update")
    update_sets()
    res = DBConnection.singleQuery("SELECT name,scryfall_id FROM Sets WHERE card_count>0")
    if res is not None:
        for name,scryfall_id in res:
            logger.info("Updating Set: %s", name)
            res = update_cards_by_set(scryfall_id)
            if res is not None:
                total_prints_added += res['NewPrints']
                total_prints_updated += res['UpdatedPrints']
                total_cards_added += res['NewCards']
                total_cards_updated += res['UpdatedCards']
                total_prices_added += res['NewPrices']

        logger.info("Finished Full Update\n\tCards Added: %d\tCards Updated: %d\n\tPrints Added: %d\tPrints Updated: %d\n\tPrices Added: %d",total_cards_added, total_cards_updated, total_prints_added, total_prints_updated, total_prices_added)

    else:
        logger.error("No Sets found!")

card_castle_re = re.compile("^(\"+[^\"]+\"+|[^,]+),(\"[^\"]+\"|[^,]+),([^,]+),(true|false),([^,]+),([^,]*),([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})0?,([0-9.]+|\"\")$")
def importCardCastle(file, date = datetime.now(), conn = None):
    close_conn = conn is None
    line_count = 0
    card_data = []
    scryfall_ids = set()
    with open(file) as f:
        for line in f:
            line_count += 1
            if line_count == 1:
                continue # skip header line
            m = card_castle_re.match(line)
            card_name = m.group(1).replace('"','')
            set_name = m.group(2).replace('"','')
            #condition = m.group(3)
            #foil = m.group(4) == "true"
            #language = m.group(5)
            multiverse_id = m.group(6)
            if multiverse_id == '':
                multiverse_id=None
            #json_id = m.group(7)
            card_data.append([card_name,set_name, m.group(3), m.group(4)=="true", m.group(5), multiverse_id, m.group(7)])
            scryfall_ids.add(m.group(7))


    card_data_size = len(card_data)
    scryfall_ids_size = len(scryfall_ids)
    if card_data_size != line_count-1:
        raise Exception("Read wrong number of cards")
    

    cursor = None
    print_ids = {}
    import_data = []
    try:
        if conn is None:
            conn = DBConnection()
        cursor = conn.getCursor()
        import_time = datetime.now()
        file_key = DBConnection.getNextId()
        cursor.execute("INSERT INTO ImportFiles(id,name,type,imported_at) VALUES(%s,%s,%s,%s)",[file_key,file.replace("import/",""),"cardcastle",import_time])
        print_id_sql = "SELECT id,scryfall_id from Prints where scryfall_id in (" + ','.join(["%s"]*len(scryfall_ids_size)) + ")"
        cursor.execute(print_id_sql,list(scryfall_ids))
        for p in cursor.fetchall():
            print_ids[p[1]]=p[0]
        for c in card_data:
            id = DBConnection.getNextId()
            import_data.append([id,print_ids[c[6]],file_key]+c)
        cursor.executemany("INSERT INTO Collection(id,print_key,file_key,card_name,set_name,card_condition,foil,language,multiverse_id,scryfall_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",import_data)
        cursor.execute("UPDATE ImportFiles SET line_count=%s where id=%s",[line_count,file_key])
        conn.commit()

    except Exception as error:
        print(error)
        traceback.print_exc()
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None and close_conn:
            conn.close()


scryfall_filename = re.compile("^([-a-z]+)-([0-9]{14}).json")
cardcastle_filename = re.compile("^export_cardcastle_(.+)_([0-9]{10}).csv")
def importFiles():
    imported_files = set()
    scryfall_data = {}
    scryfall_rulings = {}
    cardcastle_data = {}
    res = DBConnection.singleQuery("SELECT name From ImportFiles")
    if res is not None:
        for f in res:
            imported_files.add(f[0])
    files = os.listdir("import")
    files = [f for f in files if os.path.isfile("import/"+f)]
    json_files = [f for f in files if f.endswith(".json")]
    csv_files = [f for f in files if f.endswith(".csv")]

    for f in json_files:
        logger.debug("Checking json file: %s", f)
        if f in imported_files:
            logger.debug("File %s has already been imported", f)
            continue
        m = scryfall_filename.match(f)
        if m is None:
            logger.error("Unknown json file type: %s", f)
            continue
        f_type = m.group(1)
        date_str = m.group(2)
        f_date = datetime(int(date_str[:4]),int(date_str[4:6]),int(date_str[6:8]),int(date_str[8:10]),int(date_str[10:12]),int(date_str[12:14]))
        if f_type in ["oracle-cards", "unique-artwork", "default-cards", "all-cards"]:
            scryfall_data[f] = [f_type, f_date]
        elif f_type == "rulings":
            scryfall_rulings[f] = [f_type, f_date]
        else:
            logger.error("Unknown json file type: %s", f)
    for f in csv_files:
        logger.debug("Checking csv file: %s", f)
        if f in imported_files:
            logger.debug("File %s has already been imported", f)
            continue
        m = cardcastle_filename.match(f)
        if m is None:
            logger.error("Unknown csv file type: %s", f)
            continue
        f_type = m.group(1)
        date_str = m.group(2)
        f_date = datetime.fromtimestamp(int(date_str))
        cardcastle_data[f] = [f_type,f_date]

    conn = None
    import_sql = "INSERT INTO ImportFiles(id,name,type,imported_at) VALUES(%s,%s,%s,%s)"
    try:
        conn = DBConnection()
        cursor = conn.getCursor()
        for file,v in scryfall_data.items():
            with open("import/"+file) as f:
                f_type = v[0]
                f_date = v[1]
                data = json.load(f)
                success = import_card_data(data,data_date=f_date,conn=conn)
                if success:
                    id = DBConnection.getNextId()
                    cursor.execute(import_sql,[id,file,"scryfall-"+f_type,datetime.now()])
                    conn.commit()
        for file,v in scryfall_rulings.items():
            with open("import/"+file) as f:
                f_type = v[0]
                f_date = v[1]
                data = json.load(f)
                success = import_ruling_data(data,date=f_date,conn=conn)
                if success:
                    id = DBConnection.getNextId()
                    cursor.execute(import_sql,[id,file,"scryfall-"+f_type,datetime.now()])
                    conn.commit()
        for file,v in cardcastle_data.items():
            f_type = v[0]
            f_date = v[1]
            importCardCastle("import/"+file,date=f_date,conn=conn)

    except Exception as error:
        print(error)
        traceback.print_exc()
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()



"""
oracle-cards-20230805210137.json
unique-artwork-20230805210239.json
default-cards-20230805210603.json
all-cards-20230805211955.json
rulings-20230805210025.json
"""


if args.action == 'update_sets':
    update_sets()
elif args.action == 'update_cards':
    update_all_cards_and_sets()
elif args.action =='import_files':
    update_sets()
    importFiles()