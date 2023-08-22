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
from MTGClasses import MTGPrint, MTGCard, MTGSet, MTGPrice, CardFace, RelatedCard

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

def import_card_data(data,set_scryfall_id = None, data_date = datetime.now(),conn=None):
    #total_prints_added = 0
    #total_prints_updated = 0
    #total_cards_added = 0
    #total_cards_updated= 0
    #total_prices_added= 0
    #curr_prints = {}
    #curr_cards = {}
    #curr_sets = {}
    #curr_prices = {}
    #new_prints = {}
    #new_cards = []
    #print_hashes = set()
    #card_hashes = set()
    #print_dates = {}
    #card_dates = {}
    #update_prints = {}
    #update_cards = []
    #price_only = {}

    #TODO: These can exist as class attributes in their classes
    #print_id_map,print_date_map,print_hashes = MTGPrint.getExistingPrintData(set_scryfall_id=set_scryfall_id)
    #card_id_map,card_date_map,card_hashes = MTGCard.getExistingCardData()
    #set_names_map = MTGSet.getSetNames(scryfall_id=set_scryfall_id)
    #price_date_map = MTGPrice.getExistingPrices(set_scryfall_id=set_scryfall_id)
    
    for p in data:
        prnt = MTGPrint(p,data_date = data_date)
        card = MTGCard(p,data_date = data_date)
        #scryfall_id = prnt.getScryfallId()

        # card name is not a unique identifier by itself as there are certain tokens that have the same name but different properties
        # oracle_id should be a unique identifer, except that reversible cards do not have an oracle_id. Instead each face will have its own oracle_id
        # however the combination of card name and oracle_id should provide a unique identifier for all cards
        ###oracle_id = prnt.getOracleId() if prnt.getOracleId() is not None else ''
        ###card_id = prnt.getName()+oracle_id
        #card_id = ''.join(filter(None,[prnt.getName(),prnt.getOracleId()]))

        #TODO: Move to classes
        #if card_id not in card_id_map:
        ##if not card.exists():
            #new_cards[card_id] = card
            ##new_cards.append(card)
        #elif card.getMD5() not in card_hashes and data_date > card_date_map[card_id]:
        ##elif card.needsUpdate():
            #update_cards[card_id] = card
            ##update_cards.append(card)

        """ if scryfall_id not in print_id_map:
            new_prints[scryfall_id] = prnt
        elif prnt.getMD5() not in print_hashes and data_date > print_date_map[scryfall_id]:
            update_prints[scryfall_id] = prnt """
        """ elif prnt.getPrices() is not None:
            print_id = print_id_map[scryfall_id]
            last_price_date = price_date_map.get(print_id)
            #logger.debug("Current Date: %s Last Price Date: %s",date,last_price_date)
            if last_price_date is not None:
                date_diff = data_date.date() - last_price_date
                #logger.debug("Date diff: %d", date_diff.days)
                if date_diff.days < 7:
                    continue

            price_only[print_id] = prnt """

    #logger.info("New Prints: %d New Cards: %d" ,len(new_prints),len(new_cards))
    #logger.info("Updated Prints: %d Updated Cards: %d" ,len(update_prints),len(update_cards))

    counts = {'NewCards': 0, 'UpdatedCards': 0, 'NewPrints': 0, 'UpdatedPrints': 0, 'NewPrices': 0}

    close_conn = conn is None
    cursor = None
    try:
        if conn is None:
            conn = DBConnection()
        cursor = conn.getCursor()
        #Persist new cards
        #card_data = []
        #persisted_cards = {}
        #persisted_card_names = []

        #card_id_sql_start = "SELECT id,name,ifnull(oracle_id,'') FROM Cards WHERE name in ("
        #batches = 0
        while(MTGCard.hasNewData()):
            new_card_data = MTGCard.getNewBatch()
            cursor.executemany(MTGCard.insert_sql,new_card_data)

            #id_sql = MTGCard.id_sql_start + "%s,"*len(new_card_data)
            #id_sql = id_sql[:-1] + ")"
            id_sql = MTGCard.id_sql_start + ','.join(["%s"]*len(new_card_data)) + ")"
            cursor.execute(id_sql,[row[0] for row in new_card_data])
            MTGCard.updateIds(new_card_data,cursor.fetchall())
            conn.commit()
            #batches += 1
            counts['NewCards']+=len(new_card_data)
            logger.info("Cards Imported: %d" ,counts['NewCards'])

        #batches = 0
        while(MTGCard.hasUpdateData()):
            update_card_data = MTGCard.getUpdateBatch()
            cursor.executemany(MTGCard.update_sql,update_card_data)
            conn.commit()
            #batches += 1
            counts['UpdatedCards']+=len(update_card_data)
            logger.info("Cards Updated: %d" ,counts['UpdatedCards'])

        """ for card_id in new_cards:
            card = new_cards[card_id]
            card_data.append(card.getPersistData()+[card.getMD5(),data_date])
            persisted_cards[card_id]=card
            persisted_card_names.append(card.getName())
            if len(card_data)==BATCH_SIZE or len(card_data)+batches*BATCH_SIZE == len(new_cards):
                #print(card_data)
                cursor.executemany(card_sql,card_data)
                card_binds = "%s,"*len(card_data)
                card_id_sql = card_id_sql_start + card_binds
                card_id_sql = card_id_sql[:-1] + ")"
                cursor.execute(card_id_sql,persisted_card_names)
                for c in cursor.fetchall():
                    if c[1]+c[2] in persisted_cards.keys():
                        card_id_map[c[1]+c[2]] = c[0]
                total_cards_added = len(card_data)+batches*BATCH_SIZE
                logger.info("Cards Imported: %d" ,total_cards_added)
                card_data = []
                persisted_cards = {}
                persisted_card_names = []
                batches += 1
                conn.commit() """

        #Persist updated cards
        """ card_update_data = []
        card_update_sql = "UPDATE Cards SET name=%s,oracle_id=%s,prints_search_uri=%s,cmc=%s,color_identity=%s,reserved=%s,type_line=%s,oracle_text=%s,color_indicator=%s,color=%s,edhc_rank=%s,loyalty=%s,mana_cost=%s,penny_rank=%s,power=%s,toughness=%s,produced_mana=%s,hand_modifier=%s,life_modifier=%s,hash=%s,update_count=update_count+1,update_time=%s"
        card_update_sql += " WHERE id = %s" 
        batches = 0
        for card_id in update_cards:
            card = update_cards[card_id]
            card_update_data.append(card.getPersistData()+[card.getMD5(),data_date,card_id_map[card_id]])
            if len(card_update_data)==BATCH_SIZE or len(card_update_data)+batches*BATCH_SIZE == len(update_cards):
                cursor.executemany(card_update_sql,card_update_data)
                total_cards_updated = len(card_update_data)+batches*BATCH_SIZE
                logger.info("Cards Updated: %d" ,total_cards_updated)
                card_update_data = []
                batches += 1
                conn.commit() """

        # Persist new prints
        #print_data = []
        #persisted_prints= {}
        #print_sql = "INSERT INTO Prints(card_key,set_key,scryfall_id,lang,rulings_uri,scryfall_uri,uri,oversized,layout,booster,border_color,card_back_id,collector_number,digital,frame,full_art,highres_image,image_status,promo,rarity,released_at,reprint,story_spotlight,textless,variation,arena_id,mtgo_id,mtgo_foil_id,tcgplayer_id,tcgplayer_etched_id,cardmarket_id,artist,content_warning,flavor_name,flavor_text,illustration_id,printed_name,printed_text,printed_type_line,variation_of,security_stamp,watermark,preview_previewed_at,preview_source_uri,preview_source,finish_nonfoil,finish_foil,finish_etched,game_paper,game_mtgo,game_arena,game_astral,game_sega,hash,update_time)"
        #print_sql += "VALUES ("+"%s,"*55
        #print_sql = print_sql[:-1] + ")"
        #print_id_sql_start = "SELECT id,scryfall_id FROM Prints WHERE scryfall_id in ("

        
        #card_face_sql = "INSERT INTO CardFaces(print_key,name,mana_cost,artist,cmc,color_indicator,colors,flavor_text,illustration_id,layout,loyalty,oracle_id,oracle_text,power,toughness,printed_name,printed_text,printed_type_line,type_line,watermark) "
        #card_face_sql += "VALUES (" + "%s,"*20
        #card_face_sql = card_face_sql[:-1] + ")"
        
        #addl_data_sql = "INSERT INTO AdditionalData(print_key,type,type2,value) VALUES (%s,%s,%s,%s)"
        #legalities_sql = "INSERT INTO Legalities(print_key,standard,future,historic,gladiator,pioneer,explorer,modern,legacy,pauper,vintage,penny,commander,oathbreaker,brawl,historicbrawl,alchemy,paupercommander,duel,oldschool,premodern,predh) "
        #legalities_sql += "VALUES (" + "%s,"*22
        #legalities_sql = legalities_sql[:-1] + ")"

        while(MTGPrint.hasNewData()):
            new_print_data, new_addl_data, new_legalities = MTGPrint.getNewBatch()
            cursor.executemany(MTGPrint.insert_sql,new_print_data)

            id_sql = MTGPrint.id_sql_start + ','.join(["%s"]*len(new_print_data)) + ")"
            scryfall_ids = [row[2] for row in new_print_data]
            cursor.execute(id_sql, scryfall_ids)
            MTGPrint.updateIds(new_print_data,cursor.fetchall())
            
            cursor.executemany(CardFace.insert_sql,CardFace.getBatchData())
            cursor.executemany(RelatedCard.insert_sql,RelatedCard.getBatchData())
            cursor.executemany(MTGPrice.insert_sql,MTGPrice.getBatchData())

            addl_data = []
            legalities_data = []
            for scryfall_id in scryfall_ids:
                print_key = MTGPrint.getPrintKey(scryfall_id)
                if new_addl_data.get(scryfall_id):
                    for data in new_addl_data.get(scryfall_id):
                        addl_data.append([print_key] + data)
                if new_legalities.get(scryfall_id):
                    #for data in new_legalities.get(scryfall_id):
                        legalities_data.append([print_key] + new_legalities.get(scryfall_id))

            cursor.executemany(MTGPrint.insert_addl_data_sql,addl_data)
            cursor.executemany(MTGPrint.insert_legalities_sql,legalities_data)

            conn.commit()
            counts['NewPrints']+=len(new_print_data)
            logger.info("Prints Imported: %d" ,counts['NewPrints'])

        while(MTGPrint.hasUpdateData()):
            update_print_data, update_addl_data, update_legalities = MTGPrint.getUpdateBatch()
            cursor.executemany(MTGPrint.update_sql,update_print_data)

            print_keys = [row[-1] for row in update_print_data]

            card_face_delete_sql = CardFace.delete_sql_start + ','.join(["%s"]*len(print_keys)) + ")"
            related_card_delete_sql = RelatedCard.delete_sql_start + ','.join(["%s"]*len(print_keys)) + ")"
            addl_data_delete_sql = MTGPrint.addl_data_delete_sql_start + ','.join(["%s"]*len(print_keys)) + ")"
            cursor.execute(card_face_delete_sql,print_keys)
            cursor.execute(related_card_delete_sql,print_keys)
            cursor.execute(addl_data_delete_sql,print_keys)

            cursor.executemany(CardFace.insert_sql,CardFace.getBatchData())
            cursor.executemany(RelatedCard.insert_sql,RelatedCard.getBatchData())

            addl_data = []
            legalities_data = []
            scryfall_ids = [row[2] for row in update_print_data]
            for scryfall_id in scryfall_ids:
                print_key = MTGPrint.getPrintKey(scryfall_id)
                if update_addl_data.get(scryfall_id):
                    for data in update_addl_data.get(scryfall_id):
                        addl_data.append([print_key] + data)
                if update_legalities.get(scryfall_id):
                    #for data in update_legalities.get(scryfall_id):
                        legalities_data.append(update_legalities.get(scryfall_id) + [print_key])
            cursor.executemany(MTGPrint.insert_addl_data_sql,addl_data)
            cursor.executemany(MTGPrint.legalities_update_sql,legalities_data)

            conn.commit()
            counts['UpdatedPrints']+=len(update_print_data)
            logger.info("Prints Updated: %d" ,counts['UpdatedPrints'])


        """ card_faces = {}
        related_cards = {}
        prices = {}
        addl_data = {}
        legalities = {}
        batches = 0
        for print_sid in new_prints:
            prnt = new_prints[print_sid]
            oracle_id=prnt.getOracleId() if prnt.getOracleId() is not None else ''
            card_key = card_id_map[prnt.getName() + oracle_id]
            #prnt.setCardKey(card_key)
            set_key = set_names_map[prnt.getSetName()]
            #prnt.setSetKey(set_key)
            print_data.append([card_key,set_key]+prnt.getPersistData()+[prnt.getMD5(),data_date])
            persisted_prints[print_sid] = prnt

            faces = prnt.getCardFaces()
            if faces is not None and len(faces)>0:
                card_faces[print_sid] = faces
            
            related = prnt.getParts()
            if related is not None and len(related)>0:
                related_cards[print_sid] = related

            prices[print_sid] = prnt.getPrices()

            addl = []
            a = prnt.getAdditionalArrays()
            if len(a)>0:
                for type in a:
                    for value in a[type]:
                        addl.append([type, None, value])
            o = prnt.getAdditionalObjects()
            if len(o)>0:
                for type in o:
                    for k, v in o[type].items():
                        addl.append([type,k,v])
            addl_data[print_sid] = addl

            legalities[print_sid] = prnt.getLegalities()
            
            if len(print_data)==BATCH_SIZE or len(print_data)+batches*BATCH_SIZE == len(new_prints):
                cursor.executemany(print_sql,print_data)
                print_binds = "%s,"*len(print_data)
                print_id_sql = print_id_sql_start + print_binds
                print_id_sql = print_id_sql[:-1] + ")"
                cursor.execute(print_id_sql,list(persisted_prints.keys()))

                new_face_data = []
                new_related_data = []
                new_price_data = []
                new_addl_data = []
                new_legalities_data = []
                for p in cursor.fetchall():
                    print_id_map[p[1]] = p[0]
                    new_faces = card_faces.get(p[1])
                    if new_faces is not None:
                        for new_face in new_faces:
                            #new_face.setPrintKey(p[0])
                            new_face_data.append([p[0]]+new_face.getPersistData())
                    
                    new_related_cards = related_cards.get(p[1])
                    if new_related_cards is not None:
                        for new_related_card in new_related_cards:
                            #new_related_card.setPrintKey(p[0])
                            new_related_data.append([p[0]]+new_related_card.getPersistData())

                    new_prices = prices.get(p[1])
                    if new_prices is not None:
                        #new_prices.setPrintKey(p[0])
                        new_price_data.append([p[0]]+new_prices.getPersistData())

                    new_addls = addl_data.get(p[1])
                    if new_addls is not None:
                        for new_addl in new_addls:
                            new_addl_data.append([p[0]]+new_addl)

                    new_legalities = legalities.get(p[1])
                    if legalities is not None:
                        new_legalities_data.append([p[0]]+new_legalities)

                cursor.executemany(card_face_sql,new_face_data)
                cursor.executemany(related_card_sql,new_related_data)
                cursor.executemany(price_sql,new_price_data)
                cursor.executemany(addl_data_sql,new_addl_data)
                cursor.executemany(legalities_sql,new_legalities_data)
            #other related data

                total_prints_added = len(print_data)+batches*BATCH_SIZE
                logger.info("Prints Imported: %d" ,total_prints_added)
                print_data = []
                persisted_prints = {}

                batches += 1
                conn.commit()

        #Persist print updates
        print_update_sql = "UPDATE Prints SET scryfall_id=%s,lang=%s,rulings_uri=%s,scryfall_uri=%s,uri=%s,oversized=%s,layout=%s,booster=%s,border_color=%s,card_back_id=%s,collector_number=%s,digital=%s,frame=%s,full_art=%s,highres_image=%s,image_status=%s,promo=%s,rarity=%s,released_at=%s,reprint=%s,story_spotlight=%s,textless=%s,variation=%s,arena_id=%s,mtgo_id=%s,mtgo_foil_id=%s,tcgplayer_id=%s,tcgplayer_etched_id=%s,cardmarket_id=%s,artist=%s,content_warning=%s,flavor_name=%s,flavor_text=%s,illustration_id=%s,printed_name=%s,printed_text=%s,printed_type_line=%s,variation_of=%s,security_stamp=%s,watermark=%s,preview_previewed_at=%s,preview_source_uri=%s,preview_source=%s,finish_nonfoil=%s,finish_foil=%s,finish_etched=%s,game_paper=%s,game_mtgo=%s,game_arena=%s,game_astral=%s,game_sega=%s,hash=%s,update_count=update_count+1,update_time=%s "
        print_update_sql += "WHERE id=%s"
        legalities_update_sql = "UPDATE Legalities SET standard=%s,future=%s,historic=%s,gladiator=%s,pioneer=%s,explorer=%s,modern=%s,legacy=%s,pauper=%s,vintage=%s,penny=%s,commander=%s,oathbreaker=%s,brawl=%s,historicbrawl=%s,alchemy=%s,paupercommander=%s,duel=%s,oldschool=%s,premodern=%s,predh=%s "
        legalities_update_sql += "WHERE id = %s"
        card_face_delete_sql_start = "DELETE FROM CardFaces where print_key in ("
        related_card_delete_sql_start = "DELETE FROM RelatedCards where print_key in ("
        addl_data_delete_sql_start = "DELETE FROM AdditionalData where print_key in ("
        print_update_data = []
        update_face_data = []
        update_related_data = []
        update_price_data = []
        update_addl_data = []
        update_legalities_data = []
        prnt_ids = []
        batches = 0
        for print_sid in update_prints:
            prnt = update_prints[print_sid]
            prnt_id = print_id_map[print_sid]
            print_update_data.append(prnt.getPersistData()+[prnt.getMD5(),data_date,prnt_id])

            faces = prnt.getCardFaces()
            if faces is not None and len(faces)>0:
                for update_face in faces:
                    #update_face.setPrintKey(prnt_id)
                    update_face_data.append([prnt_id]+update_face.getPersistData())
            
            related = prnt.getParts()
            if related is not None and len(related)>0:
                for update_related_card in related:
                    #update_related_card.setPrintKey(prnt_id)
                    update_related_data.append([prnt_id]+update_related_card.getPersistData())

            update_prices = prnt.getPrices()
            if update_prices is not None:
                #update_prices.setPrintKey(prnt_id)
                update_price_data.append([prnt_id]+update_prices.getPersistData())

            a = prnt.getAdditionalArrays()
            if len(a)>0:
                for type in a:
                    for value in a[type]:
                        update_addl_data.append([prnt_id, type, None, value])
            o = prnt.getAdditionalObjects()
            if len(o)>0:
                for type in o:
                    for k, v in o[type].items():
                        update_addl_data.append([prnt_id, type,k,v])

            update_legalities_data.append([prnt_id]+prnt.getLegalities())

            prnt_ids.append(prnt_id)

            if len(print_update_data)==BATCH_SIZE or len(print_update_data)+batches*BATCH_SIZE == len(update_prints):
                card_face_delete_sql = card_face_delete_sql_start + "%s,"*len(prnt_ids)
                card_face_delete_sql = card_face_delete_sql[:-1] + ")"
                related_card_delete_sql = related_card_delete_sql_start + "%s,"*len(prnt_ids)
                related_card_delete_sql = related_card_delete_sql[:-1] + ")"
                addl_data_delete_sql = addl_data_delete_sql_start + "%s,"*len(prnt_ids)
                addl_data_delete_sql = addl_data_delete_sql[:-1] + ")"

                cursor.execute(card_face_delete_sql,prnt_ids)
                cursor.execute(related_card_delete_sql,prnt_ids)
                cursor.execute(addl_data_delete_sql,prnt_ids)

                cursor.executemany(print_update_sql,print_update_data)
                cursor.executemany(legalities_update_sql,update_legalities_data)

                cursor.executemany(card_face_sql,update_face_data)
                cursor.executemany(related_card_sql,update_related_data)
                cursor.executemany(price_sql,update_price_data)
                cursor.executemany(addl_data_sql,update_addl_data)

                total_prints_updated = len(print_update_data)+batches*BATCH_SIZE
                logger.info("Prints Updated: %d" ,total_prints_updated)

                print_update_data = []
                update_face_data = []
                update_related_data = []
                update_price_data = []
                update_addl_data = []
                update_legalities_data = []
                prnt_ids = []

                batches += 1
                conn.commit() """

        # Update prices
        while(MTGPrice.hasData()):
            price_data = MTGPrice.getBatch()
            cursor.executemany(MTGPrice.insert_sql,price_data)
            conn.commit()
            counts['NewPrices']+=len(price_data)
            logger.info("Prices Added: %d" ,counts['NewPrices'])
        """ price_data = []
        batches = 0
        for print_id,prnt in price_only.items():
            if prnt.getPrices() is None:
                continue
            price_data.append([print_id]+prnt.getPrices().getPersistData())
            if len(price_data)==BATCH_SIZE or len(price_data)+batches*BATCH_SIZE == len(price_only):
                cursor.executemany(price_sql,price_data)
                total_prices_added = len(price_data)+batches*BATCH_SIZE
                logger.info("Prices Added: %d" ,total_prices_added)
                price_data = []
                batches += 1 """
        
        conn.commit()        


    except Exception as error:
        print(error)
        traceback.print_exc()
        if conn is not None:
            conn.rollback()
        raise(error)
    finally:
        if conn is not None and close_conn:
            conn.close()

    #return [total_prints_added, total_prints_updated, total_cards_added, total_cards_updated, total_prices_added]
    return counts


def update_sets():
    set_data = scryfall_api_request('sets')
    sets = []
    for s in set_data:
        sets.append(MTGSet(s))
    logger.info("Total sets retrieved from scryfall: %d" ,len(sets))
    #curr_sets = {}
    #set_types = {}
    #set_hashes = []

    conn = None
    cursor = None
    try:
        conn = DBConnection()
        cursor = conn.getCursor()
        """ cursor.execute("SELECT id,scryfall_id,hash FROM Sets")
        for s in cursor.fetchall():
            curr_sets[s[1]]=s[0]
            set_hashes.append(s[2]) """

        """ cursor.execute("SELECT id,type FROM SetTypes")
        for s in cursor.fetchall():
            set_types[s[1]]=s[0] """

        #logger.info("Retrieve %d sets from DB" ,len(curr_sets))

        """ data = []
        update_data = []
        for set in sets:
            logger.info("Checking: %s" ,set.name)
            set_hash = set.getMD5()
            if set.scryfall_id not in curr_sets.keys():
                logger.info("%s not found in DB. Persisting" ,set.name)
                #data.append([set.scryfall_id,set.code,set.name,set.card_count,set.digital,set.foil_only,set.nonfoil_only,
                #             set.scryfall_uri,set.uri,set.icon_svg_uri,set.search_uri,set.mtgo_code,set.tcgplayer_id,set.released_at,set.block_code,
                #             set.block,set.parent_set_code,set.printed_size,set_types.get(set.set_type)])
                data.append(set.getPersistData()+[set_types.get(set.set_type),set_hash])

            elif set_hash not in set_hashes:
                logger.info("%s needs updating" ,set.name)
                update_data.append(set.getPersistData()+[set_types.get(set.set_type),set_hash,curr_sets[set.scryfall_id]]) """
                
        while(MTGSet.hasNewData()):
            new_set_data = MTGSet.getNewBatch()
            cursor.executemany(MTGSet.insert_sql,new_set_data)

            id_sql = MTGSet.id_sql_start + ','.join(["%s"]*len(new_set_data)) + ")"
            cursor.execute(id_sql,[row[0] for row in new_set_data])
            MTGSet.updateIds(cursor.fetchall())
            
            conn.commit()
            logger.info("Sets Imported: %d" ,len(new_set_data))

        while(MTGSet.hasUpdateData()):
            update_set_data = MTGSet.getUpdateBatch()
            cursor.executemany(MTGSet.update_sql,update_set_data)
            conn.commit()
            logger.info("Sets Updated: %d" ,len(update_set_data))
        
        """ if len(data)>0:
            print("Persisting %d new sets" % len(data))
            insert_sql = "INSERT INTO Sets(scryfall_id,code,name,card_count,digital,foil_only,nonfoil_only,scryfall_uri,uri,icon_svg_uri,"
            insert_sql += "search_uri,mtgo_code,tcgplayer_id,released_at,block_code,block,parent_set_code,printed_size,SetType_key,hash) VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s,%s)"
            cursor.executemany(insert_sql,data)

        if len(update_data)>0:
            print("Updating %d existing sets" % len(update_data))
            update_sql = "UPDATE Sets set scryfall_id=%s,code=%s,name=%s,card_count=%s,digital=%s,foil_only=%s,nonfoil_only=%s,scryfall_uri=%s,uri=%s,icon_svg_uri=%s,"
            update_sql += "search_uri=%s,mtgo_code=%s,tcgplayer_id=%s,released_at=%s,block_code=%s,block=%s,parent_set_code=%s,printed_size=%s,SetType_key=%s,hash=%s,update_count=update_count+1 WHERE id=%s"
            cursor.executemany(update_sql,update_data) """

        #conn.commit()

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
            ret = import_card_data(set_data,set_scryfall_id=scryfall_id)
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

                 #counts = {'NewCards': 0, 'UpdatedCards': 0, 'NewPrints': 0, 'UpdatedPrints': 0, 'NewPrices': 0}

        logger.info("Finished Full Update\n\tCards Added: %d\tCards Updated: %d\n\tPrints Added: %d\tPrints Updated: %d\n\tPrices Added: %d",total_cards_added, total_cards_updated, total_prints_added, total_prints_updated, total_prices_added)

    else:
        logger.error("No Sets found!")
#Card Name,Set Name,Condition,Foil,Language,Multiverse ID,JSON ID,Price USD
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
            #print(line)
            m = card_castle_re.match(line)
            #print("Line: %s" % (line))
            #print(m.groups())
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
        cursor.execute("INSERT INTO ImportFiles(name,type,imported_at) VALUES(%s,%s,%s)",[file.replace("import/",""),"cardcastle",import_time])
        cursor.execute("SELECT id from ImportFiles where name=%s and type=%s",[file.replace("import/",""),"cardcastle"])
        file_key = cursor.fetchone()[0]
        print_id_sql = "SELECT id,scryfall_id from Prints where scryfall_id in ("
        print_id_sql += "%s,"*scryfall_ids_size
        print_id_sql = print_id_sql[:-1] + ")"
        cursor.execute(print_id_sql,list(scryfall_ids))
        for p in cursor.fetchall():
            print_ids[p[1]]=p[0]
        for c in card_data:
            id = [print_ids[c[6]]]+c
            #print(id)
            import_data.append([print_ids[c[6]],file_key]+c)
        cursor.executemany("INSERT INTO Collection(print_key,file_key,card_name,set_name,card_condition,foil,language,multiverse_id,scryfall_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",import_data)
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
    import_sql = "INSERT INTO ImportFiles(name,type,imported_at) VALUES(%s,%s,%s)"
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
                    cursor.execute(import_sql,[file,"scryfall-"+f_type,datetime.now()])
                    conn.commit()
        for file,v in scryfall_rulings.items():
            with open("import/"+file) as f:
                f_type = v[0]
                f_date = v[1]
                data = json.load(f)
                success = import_ruling_data(data,date=f_date,conn=conn)
                if success:
                    cursor.execute(import_sql,[file,"scryfall-"+f_type,datetime.now()])
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



#update_sets()
#with open("import/all-cards-20230723212733.json") as f:
#    data = json.load(f)
#    print(len(data))
#    import_card_data(data)

#update_cards_by_set('4d92a8a7-ccb0-437d-abdc-9d70fc5ed672')
#update_all_cards_and_sets()

#importCardCastle()
#update_sets()
#importFiles()

if args.action == 'update_sets':
    update_sets()
elif args.action == 'update_cards':
    update_all_cards_and_sets()
elif args.action =='import_files':
    update_sets()
    importFiles()