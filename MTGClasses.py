import hashlib
import logging
from datetime import datetime
from DB import DBConnection

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('MTG')

BATCH_SIZE = 1000
PRICE_PERIOD_DAYS = 7

class MTGSet():
    existing_sql = "SELECT id,scryfall_id,hash FROM Sets"
    #set_names_sql_end = " WHERE scryfall_id = %s"
    set_type_sql = "SELECT id,type FROM SetTypes"
    id_sql_start = "SELECT id,scryfall_id FROM Sets WHERE scryfall_id in ("

    insert_sql = "INSERT INTO Sets(scryfall_id,code,name,card_count,digital,foil_only,nonfoil_only,scryfall_uri,uri,icon_svg_uri,search_uri,mtgo_code,tcgplayer_id,released_at,block_code,block,parent_set_code,printed_size,SetType_key,hash) VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s,%s)"
    update_sql = "UPDATE Sets set scryfall_id=%s,code=%s,name=%s,card_count=%s,digital=%s,foil_only=%s,nonfoil_only=%s,scryfall_uri=%s,uri=%s,icon_svg_uri=%s,search_uri=%s,mtgo_code=%s,tcgplayer_id=%s,released_at=%s,block_code=%s,block=%s,parent_set_code=%s,printed_size=%s,SetType_key=%s,hash=%s,update_count=update_count+1 WHERE id=%s"

    _id_map = {}
    _hashes = set()
    _set_types = {}

    result = DBConnection.singleQuery(existing_sql)
    for s in result:
        _id_map[s[1]]=s[0]
        _hashes.add(s[2])

    result = DBConnection.singleQuery(set_type_sql)
    for s in result:
        _set_types[s[1]]=s[0]

    _new_sets = []
    _update_sets = []

    def __init__(self,data):
        self.scryfall_id = data.get('id')
        self.code = data.get('code')
        self.name = data.get('name')
        self.set_type = data.get('set_type')
        self.card_count = int(data.get('card_count'))
        self.digital = data.get('digital')=="true"
        self.foil_only = data.get('foil_only')=="true"
        self.nonfoil_only = data.get('nonfoil_only')=="true"
        self.scryfall_uri = data.get('scryfall_uri')
        self.uri = data.get('uri')
        self.icon_svg_uri = data.get('icon_svg_uri')
        self.search_uri = data.get('search_uri')

        self.mtgo_code = data.get('mtgo_code')
        self.tcgplayer_id = data.get('tcgplayer_id')
        self.released_at = data.get('released_at')
        self.block_code = data.get('block_code')
        self.block = data.get('block')
        self.parent_set_code = data.get('parent_set_code')
        self.printed_size = data.get('printed_size')

        self.md5 = hashlib.md5(''.join(str(field) for field in self.getPersistData()).encode('utf-8')).hexdigest()

        if not self.exists():
            logger.info("%s not found in DB. Persisting" ,self.name)
            MTGSet._new_sets.append(self)
            MTGSet._hashes.add(self.md5)
        elif self.needsUpdate():
            logger.info("%s needs updating" ,self.name)
            MTGSet._update_sets.append(self)
            MTGSet._hashes.add(self.md5)

    def exists(self):
        return self.scryfall_id in MTGSet._id_map
    
    def needsUpdate(self):
        return self.exists() and self.getMD5() not in MTGSet._hashes
    
    @staticmethod
    def getSetKey(set_scryfall_id):
        return MTGSet._id_map.get(set_scryfall_id)
    
    @staticmethod
    def getSetTypeKey(set_type):
        return MTGSet._set_types.get(set_type)
    
    def getScryfallId(self):
        return self.scryfall_id
    
    def getSetType(self):
        return self.set_type

    def getMD5(self):
        return self.md5

    def getPersistData(self):
        return [self.scryfall_id,self.code,self.name,self.card_count,self.digital,self.foil_only,self.nonfoil_only,
                self.scryfall_uri,self.uri,self.icon_svg_uri,self.search_uri,self.mtgo_code,self.tcgplayer_id,self.released_at,self.block_code,
                self.block,self.parent_set_code,self.printed_size]
    
    @staticmethod
    def hasNewData():
        return len(MTGSet._new_sets) > 0
    
    @staticmethod
    def hasUpdateData():
        return len(MTGSet._update_sets) > 0
    
    @staticmethod
    def getNewBatch():
        return MTGSet.getBatch(MTGSet._new_sets)
    
    @staticmethod
    def getUpdateBatch():
        set_data = []
        for s in  MTGSet.getBatch(MTGSet._update_sets):
            set_data.append(s + [MTGSet.getSetKey(s.getScryfallId())])
        return set_data

    @staticmethod
    def getBatch(set_type):
        set_data = []
        batch_sets = set_type[:BATCH_SIZE]
        del set_type[:BATCH_SIZE]

        for s in batch_sets:
            set_data.append(s.getPersistData() + [MTGSet.getSetTypeKey(s.getSetType()),s.getMD5()])

        return set_data
        
    @staticmethod
    def updateIds(results):
        for s in results:
            MTGSet._id_map[s[1]]=s[0]


class MTGPrint():
    #existing_prints_start_sql = "SELECT p.id,p.scryfall_id,p.hash,p.update_time FROM Prints p"
    #existing_prints_end_sql = " , Sets s WHERE p.set_key = s.id and s.scryfall_id = %s"
    existing_sql = ("SELECT p.id,p.scryfall_id,p.hash,p.update_time,s.scryfall_id FROM Prints p , Sets s, Sets cs "
                    "WHERE p.set_key = s.id and s.parent_set_code=cs.parent_set_code and cs.scryfall_id = %s "
                    "UNION ALL "
                    "SELECT p.id,p.scryfall_id,p.hash,p.update_time,s.scryfall_id FROM Prints p , Sets s, Sets cs "
                    "WHERE p.set_key = s.id and cs.parent_set_code=s.code and cs.scryfall_id = %s "
                    "UNION ALL "
                    "SELECT p.id,p.scryfall_id,p.hash,p.update_time,s.scryfall_id FROM Prints p , Sets s, Sets cs "
                    "WHERE p.set_key = s.id and cs.code=s.parent_set_code and cs.scryfall_id = %s "
                    "UNION ALL "
                    "SELECT p.id,p.scryfall_id,p.hash,p.update_time,s.scryfall_id FROM Prints p , Sets s "
                    "WHERE p.set_key = s.id and s.parent_set_code is null and s.scryfall_id = %s")

    insert_sql = "INSERT INTO Prints(card_key,set_key,scryfall_id,lang,rulings_uri,scryfall_uri,uri,oversized,layout,booster,border_color,card_back_id,collector_number,digital,frame,full_art,highres_image,image_status,promo,rarity,released_at,reprint,story_spotlight,textless,variation,arena_id,mtgo_id,mtgo_foil_id,tcgplayer_id,tcgplayer_etched_id,cardmarket_id,artist,content_warning,flavor_name,flavor_text,illustration_id,printed_name,printed_text,printed_type_line,variation_of,security_stamp,watermark,preview_previewed_at,preview_source_uri,preview_source,finish_nonfoil,finish_foil,finish_etched,game_paper,game_mtgo,game_arena,game_astral,game_sega,hash,update_time) VALUES (" + ','.join(["%s"]*55) + ')'
    insert_addl_data_sql = "INSERT INTO AdditionalData(print_key,type,type2,value) VALUES (%s,%s,%s,%s)"
    insert_legalities_sql = "INSERT INTO Legalities(print_key,standard,future,historic,gladiator,pioneer,explorer,modern,legacy,pauper,vintage,penny,commander,oathbreaker,brawl,historicbrawl,alchemy,paupercommander,duel,oldschool,premodern,predh) VALUES (" + ','.join(["%s"]*22) +")"

    update_sql = "UPDATE Prints SET card_key=%s,set_key=%s,scryfall_id=%s,lang=%s,rulings_uri=%s,scryfall_uri=%s,uri=%s,oversized=%s,layout=%s,booster=%s,border_color=%s,card_back_id=%s,collector_number=%s,digital=%s,frame=%s,full_art=%s,highres_image=%s,image_status=%s,promo=%s,rarity=%s,released_at=%s,reprint=%s,story_spotlight=%s,textless=%s,variation=%s,arena_id=%s,mtgo_id=%s,mtgo_foil_id=%s,tcgplayer_id=%s,tcgplayer_etched_id=%s,cardmarket_id=%s,artist=%s,content_warning=%s,flavor_name=%s,flavor_text=%s,illustration_id=%s,printed_name=%s,printed_text=%s,printed_type_line=%s,variation_of=%s,security_stamp=%s,watermark=%s,preview_previewed_at=%s,preview_source_uri=%s,preview_source=%s,finish_nonfoil=%s,finish_foil=%s,finish_etched=%s,game_paper=%s,game_mtgo=%s,game_arena=%s,game_astral=%s,game_sega=%s,hash=%s,update_count=update_count+1,update_time=%s WHERE id=%s"
    legalities_update_sql = "UPDATE Legalities SET standard=%s,future=%s,historic=%s,gladiator=%s,pioneer=%s,explorer=%s,modern=%s,legacy=%s,pauper=%s,vintage=%s,penny=%s,commander=%s,oathbreaker=%s,brawl=%s,historicbrawl=%s,alchemy=%s,paupercommander=%s,duel=%s,oldschool=%s,premodern=%s,predh=%s WHERE print_key = %s"
    addl_data_delete_sql_start = "DELETE FROM AdditionalData where print_key in ("

    id_sql_start = "SELECT id,scryfall_id,hash,update_time FROM Prints WHERE scryfall_id in ("

    id_map = {}
    date_map = {}
    hashes = set()
    _cached_sets = set()
    
    _new_prints = []
    _update_prints = []

    def __init__(self,data, data_date = datetime.now()):
        self.data_date = data_date

        # Read JSON Data
        self.scryfall_id = data.get('id')
        self.lang = data.get('lang')
        self.rulings_uri = data.get('rulings_uri')
        self.scryfall_uri = data.get('scryfall_uri')
        self.uri = data.get('uri')
        self.oversized = data.get('oversized')=="true"
        self.layout = data.get('layout')
        self.booster = data.get('booster')
        self.border_color = data.get('border_color')
        self.card_back_id = data.get('card_back_id')
        self.collector_number = data.get('collector_number')
        self.digital = data.get('digital')=="true"
        self.frame = data.get('frame')
        self.full_art = data.get('full_art')=="true"
        self.highres_image = data.get('highres_image')=="true"
        self.image_status = data.get('image_status')
        self.promo = data.get('promo')=="true"
        self.rarity = data.get('rarity')
        self.released_at = data.get('released_at')
        self.reprint = data.get('reprint')=="true"
        self.story_spotlight = data.get('story_spotlight')=="true"
        self.textless = data.get('textless')=="true"
        self.variation = data.get('variation')=="true"
        self.set_name = data.get('set_name')
        self.set_scryfall_id = data.get('set_id')

        # Nullable Fields
        self.arena_id = data.get('arena_id')
        self.mtgo_id = data.get('mtgo_id')
        self.mtgo_foil_id = data.get('mtgo_foil_id')
        self.tcgplayer_id = data.get('tcgplayer_id')
        self.tcgplayer_etched_id = data.get('tcgplayer_etched_id')
        self.cardmarket_id = data.get('cardmarket_id')
        self.artist = data.get('artist')
        self.content_warning = data.get('content_warning')=="true"
        self.flavor_name = data.get('flavor_name')
        self.flavor_text = data.get('flavor_text')
        self.illustration_id = data.get('illustration_id')
        self.printed_name = data.get('printed_name')
        self.printed_text = data.get('printed_text')
        self.printed_type_line = data.get('printed_type_line')
        self.variation_of = data.get('variation_of')
        self.security_stamp = data.get('security_stamp')
        self.watermark = data.get('watermark')
        self.preview_previewed_at = data.get('preview.previewed_at')
        self.preview_source_uri = data.get('preview.source_uri')
        self.preview_source = data.get('preview.source')

        #Arrays
        finishes = data.get('finishes')
        self.finish_nonfoil = finishes is not None and 'nonfoil' in finishes
        self.finish_foil = finishes is not None and'foil' in finishes
        self.finish_etched = finishes is not None and 'etched' in finishes

        games = data.get('games')
        self.game_paper = games is not None and 'paper' in games
        self.game_mtgo = games is not None and 'mtgo' in games
        self.game_arena = games is not None and 'arena' in games
        self.game_astral = games is not None and 'astral' in games
        self.game_sega = games is not None and 'sega' in games

        self.additional_arrays = {}
        self.multiverse_ids = data.get('multiverse_ids')
        if self.multiverse_ids is not None:
            self.additional_arrays['multiverse_ids'] = self.multiverse_ids
        self.keywords = data.get('keywords')
        if self.keywords is not None:
            self.additional_arrays['keywords'] = self.keywords
        self.frame_effects = data.get('frame_effects')
        if self.frame_effects is not None:
            self.additional_arrays['frame_effects'] = self.frame_effects
        self.promo_types = data.get('promo_types')
        if self.promo_types is not None:
            self.additional_arrays['promo_types'] = self.promo_types
        self.attraction_lights = data.get('attraction_lights')
        if self.attraction_lights is not None:
            self.additional_arrays['attraction_lights'] = self.attraction_lights

        #Objects
        self.additional_objects = {}
        self.related_uris = data.get('related_uris')
        if self.related_uris is not None:
            self.additional_objects['related_uris'] = self.related_uris
        # image_uris can be constructed from scryfall_id
        # no need to persist these separately
        #self.image_uris = data.get('image_uris')
        #if self.image_uris is not None:
        #    self.additional_objects['image_uris'] = self.image_uris
        self.purchase_uris = data.get('purchase_uris')
        if self.purchase_uris is not None:
            self.additional_objects['purchase_uris'] = self.purchase_uris
        self.legalities = data.get('legalities')

        #Card Faces
        self.faces = None
        card_faces = data.get('card_faces')
        if card_faces is not None and len(card_faces)>0:
            self.faces = []
            for face in card_faces:
                self.faces.append(CardFace(face))

        #Related Cards
        self.parts = None
        all_parts = data.get('all_parts')
        if all_parts is not None and len(all_parts)>0:
            self.parts = []
            for part in all_parts:
                self.parts.append(RelatedCard(part))

        self.name = data.get('name')
        self.oracle_id = data.get('oracle_id')
        self.card_id = ''.join(filter(None,[self.name,self.oracle_id]))
        self.md5 = None

        # Cache prints for this set if not already done
        if self.set_scryfall_id not in MTGPrint._cached_sets:
            #existing_sql = MTGPrint.existing_prints_start_sql + MTGPrint.existing_prints_end_sql
            result = DBConnection.singleQuery(MTGPrint.existing_sql,[self.set_scryfall_id,self.set_scryfall_id,self.set_scryfall_id,self.set_scryfall_id])
            # TODO: Add error logging if we get no results
            for p in result:
                MTGPrint.id_map[p[1]]=p[0]
                MTGPrint.hashes.add(p[2])
                MTGPrint.date_map[p[1]] = p[3]
                if p[4] not in MTGPrint._cached_sets:
                    MTGPrint._cached_sets.add(p[4])
            # If, for some reason, we get no results for a set, cache the set anyway to prevent re-querying on every print
            MTGPrint._cached_sets.add(self.set_scryfall_id)

        if not self.exists():
            MTGPrint._new_prints.append(self)
        elif self.needsUpdate():
            MTGPrint._update_prints.append(self)

        self.id = MTGPrint.id_map.get(self.scryfall_id)
        #Prices
        self.price = MTGPrice(data.get('prices'),data_date,self.id,self.set_scryfall_id)
        if self.price.isNull():
            self.price = None

    def exists(self):
        return self.scryfall_id in MTGPrint.id_map
    
    def needsUpdate(self):
        return self.exists() and self.getMD5() not in MTGPrint.hashes and self.data_date > MTGPrint.date_map[self.scryfall_id]

    def getName(self):
        return self.name
    
    def getSetName(self):
        return self.set_name
    
    def getScryfallId(self):
        return self.scryfall_id
    
    def getSetScryfallId(self):
        return self.set_scryfall_id
    
    def getOracleId(self):
        return self.oracle_id
    
    def getCardId(self):
        return self.card_id
    
    def getDate(self):
        return self.data_date
    
    def getCardFaces(self):
        return self.faces
    
    def getParts(self):
        return self.parts
    
    def getPrices(self):
        return self.price
    
    def getAdditionalArrays(self):
        return self.additional_arrays
    
    def getAdditionalObjects(self):
        return self.additional_objects
    
    def getMD5(self):
        if self.md5 is None:
            hash_data = self.getPersistData()
            hash_data.extend(self.set_scryfall_id)
            hash_data.extend(self.getLegalities())
            faces = self.getCardFaces()
            if faces is not None:
                for face in faces:
                    hash_data.extend(face.getPersistData())
            parts = self.getParts()
            if parts is not None:
                for part in parts:
                    hash_data.extend(part.getPersistData())
            hash_data.extend(self.getAdditionalArrays())
            self.md5 = hashlib.md5(''.join(str(field) for field in hash_data).encode('utf-8')).hexdigest()
        return self.md5
    
    def getLegalities(self):
        return [self.legalities['standard'],self.legalities['future'],self.legalities['historic'],self.legalities['gladiator'],
                self.legalities['pioneer'],self.legalities['explorer'],self.legalities['modern'],self.legalities['legacy'],
                self.legalities['pauper'],self.legalities['vintage'],self.legalities['penny'],self.legalities['commander'],
                self.legalities['oathbreaker'],self.legalities['brawl'],self.legalities['historicbrawl'],self.legalities['alchemy'],
                self.legalities['paupercommander'],self.legalities['duel'],self.legalities['oldschool'],self.legalities['premodern'],
                self.legalities['predh']]
    
    def getPersistData(self):
        #return [self.card_key,self.set_key,self.scryfall_id,self.lang,self.rulings_uri,self.scryfall_uri,self.uri,self.oversized,
        return [self.scryfall_id,self.lang,self.rulings_uri,self.scryfall_uri,self.uri,self.oversized,
                self.layout,self.booster,self.border_color,self.card_back_id,self.collector_number,self.digital,self.frame,
                self.full_art,self.highres_image,self.image_status,self.promo,self.rarity,self.released_at,self.reprint,
                self.story_spotlight,self.textless,self.variation,self.arena_id,self.mtgo_id,self.mtgo_foil_id,self.tcgplayer_id,
                self.tcgplayer_etched_id,self.cardmarket_id,self.artist,self.content_warning,self.flavor_name,self.flavor_text,
                self.illustration_id,self.printed_name,self.printed_text,self.printed_type_line,self.variation_of,self.security_stamp,
                self.watermark,self.preview_previewed_at,self.preview_source_uri,self.preview_source,self.finish_nonfoil,self.finish_foil,
                self.finish_etched,self.game_paper,self.game_mtgo,self.game_arena,self.game_astral,self.game_sega]

    def setCardKey(self,card_key):
        self.card_key = card_key

    def setSetKey(self,set_key):
        self.set_key = set_key

    @staticmethod
    def getPrintKey(scryfall_id):
        return MTGPrint.id_map.get(scryfall_id)
    
    @staticmethod
    def hasNewData():
        return len(MTGPrint._new_prints) > 0
    
    @staticmethod
    def hasUpdateData():
        return len(MTGPrint._update_prints) > 0
    
    @staticmethod
    def getNewBatch():
        return MTGPrint.getBatch(MTGPrint._new_prints)
    
    @staticmethod
    def getUpdateBatch():
        print_data = []
        batch_data, addl_data, legalities = MTGPrint.getBatch(MTGPrint._update_prints)
        for prnt in  batch_data:
            print_data.append(prnt + [MTGPrint.getPrintKey(prnt[2])])
        return print_data, addl_data, legalities 

    #TODO: these batches can be updated to work better
    #TODO: need to clear price batch as its doesn't get called on update

    @staticmethod
    def getBatch(print_type):
        print_data = []
        addl_data = {}
        legalities = {}

        batch_prints = print_type[:BATCH_SIZE]
        del print_type[:BATCH_SIZE]

        for prnt in batch_prints:
            card_key = MTGCard.getCardKey(prnt.getCardId())
            #set_key = MTGSet.getSetKey(prnt.getSetName())
            set_key = MTGSet.getSetKey(prnt.getSetScryfallId())
            scryfall_id = prnt.getScryfallId()
            print_data.append([card_key,set_key] + prnt.getPersistData() + [prnt.getMD5(),prnt.getDate()])
            
            #faces[scryfall_id] = prnt.getCardFaces()
            #related_cards[scryfall_id] = prnt.getParts()
            #prices[scryfall_id] = prnt.getPrices()
            CardFace.addToBatch(scryfall_id,prnt.getCardFaces())
            RelatedCard.addToBatch(scryfall_id,prnt.getParts())
            MTGPrice.addToBatch(scryfall_id,prnt.getPrices())
            
            legalities[scryfall_id] = prnt.getLegalities()
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
            addl_data[scryfall_id] = addl


        return print_data, addl_data, legalities
    
    @staticmethod
    def updateIds(new_print_data,results):
        for p in results:
            MTGPrint.id_map[p[1]] = p[0]
            MTGPrint.hashes.add(p[2])
            MTGPrint.date_map[p[1]] = p[3]

        """ data = {}
        for np in new_print_data:
            data[np[2]] = [np[-2],np[-1]]
        for p in results:
            r = data.get(p[1])
            if r is not None:
                MTGPrint.id_map[p[1]] = p[0]
                MTGPrint.hashes.add(r[0])
                MTGPrint.date_map[p[1]] = r[1] """


class MTGPrice():
    existing_price_sql_start = "SELECT pr.print_key,max(pr.price_date) FROM Prices pr"
    existing_price_sql_mid = ", Prints p, Sets s WHERE pr.print_key = p.id and p.set_key = s.id and s.scryfall_id = %s"
    existing_price_sql_end = " GROUP BY pr.print_key"
    insert_sql = "INSERT INTO Prices(print_key,price_date,usd,usd_foil,usd_etched,eur,eur_foil,tix) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"

    _date_map = {}
    _cached_sets = set()
    _new_prices = []
    _batch_data = {}

    def __init__(self,data, data_date = datetime.now(),print_key = None,set_scryfall_id = None):
        self.price_date = data_date.date()
        self.usd = None if data.get('usd') is None else float(data.get('usd'))
        self.usd_foil = None if data.get('usd_foil') is None else float(data.get('usd_foil'))
        self.usd_etched = None if data.get('usd_etched') is None else float(data.get('usd_etched'))
        self.eur = None if data.get('eur') is None else float(data.get('eur'))
        self.eur_foil = None if data.get('eur_foil') is None else float(data.get('eur_foil'))
        self.tix = None if data.get('tix') is None else float(data.get('tix'))

        if print_key and not self.isNull():
            if set_scryfall_id not in MTGPrice._cached_sets:
                existing_sql = MTGPrice.existing_price_sql_start
                binds = None
                if set_scryfall_id is not None:
                    binds = [set_scryfall_id]
                    existing_sql += MTGPrice.existing_price_sql_mid
                existing_sql += MTGPrice.existing_price_sql_end
                result = DBConnection.singleQuery(existing_sql,binds)
                for p in result:
                    MTGPrice._date_map[p[0]]=p[1]
                MTGPrice._cached_sets.add(set_scryfall_id)

            last_price_date = MTGPrice._date_map.get(print_key)
            if last_price_date is None or (self.price_date - last_price_date).days > PRICE_PERIOD_DAYS:
                MTGPrice._new_prices.append([print_key] + self.getPersistData())
            

    def isNull(self):
        return self.usd is None and self.usd_foil is None and self.usd_etched is None and self.eur is None and self.eur_foil is None and self.tix is None


 #      price_sql = "INSERT INTO Prices(print_key,price_date,usd,usd_foil,usd_etched,eur,eur_foil,tix) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
 
    def getPersistData(self):
        #return [self.print_key,self.price_date,self.usd,self.usd_foil,self.usd_etched,self.eur,self.eur_foil,self.tix]
        return [self.price_date,self.usd,self.usd_foil,self.usd_etched,self.eur,self.eur_foil,self.tix]

    def setPrintKey(self,print_key):
        self.print_key = print_key

    @staticmethod
    def addToBatch(scryfall_id,prices):
        if prices is not None:
            MTGPrice._batch_data[scryfall_id] = prices.getPersistData()

    @staticmethod
    def getBatchData():
        batch_data = []
        for scryfall_id,data in MTGPrice._batch_data.items():
            print_key = MTGPrint.getPrintKey(scryfall_id)
            batch_data.append([print_key] + data)
        MTGPrice._batch_data = {}
        return batch_data
    
    @staticmethod
    def hasData():
        return len(MTGPrice._new_prices) > 0
    
    @staticmethod
    def getBatch():
        price_data = MTGPrice._new_prices[:BATCH_SIZE]
        del MTGPrice._new_prices[:BATCH_SIZE]
        return price_data


class CardFace():
    insert_sql = "INSERT INTO CardFaces(print_key,name,mana_cost,artist,cmc,color_indicator,colors,flavor_text,illustration_id,layout,loyalty,oracle_id,oracle_text,power,toughness,printed_name,printed_text,printed_type_line,type_line,watermark) VALUES (" + ','.join(["%s"]*20) + ")"
    delete_sql_start = "DELETE FROM CardFaces where print_key in ("

    _batch_data = {}

    def __init__(self,data):
        self.name = data.get('name')
        self.mana_cost = data.get('mana_cost')

        self.artist = data.get('artist')
        self.cmc = data.get('cmc')
        self.color_indicator = data.get('color_indicator')
        if self.color_indicator is not None:
            self.color_indicator = ''.join(self.color_indicator)
        self.colors = data.get('colors')
        if self.colors is not None:
            self.colors = ''.join(self.colors)
        self.flavor_text = data.get('flavor_text')
        self.illustration_id = data.get('illustration_id')
        self.image_uris = data.get('image_uris')
        self.layout = data.get('layout')
        self.loyalty = data.get('loyalty')
        self.oracle_id = data.get('oracle_id')
        self.oracle_text = data.get('oracle_text')
        self.power = data.get('power')
        self.toughness = data.get('toughness')
        self.printed_name = data.get('printed_name')
        self.printed_text = data.get('printed_text')
        self.printed_type_line = data.get('printed_type_line')
        self.type_line = data.get('type_line')
        self.watermark = data.get('watermark')

 #      card_face_sql = "INSERT INTO CardFaces(print_key,name,mana_cost,artist,cmc,color_indicator,colors,flavor_text,illustration_id,layout,loyalty,oracle_id,oracle_text,power,toughness,printed_name,printed_text,printed_type_line,type_line,watermark) "

    def getPersistData(self):
        #return [self.print_key,self.name,self.mana_cost,self.artist,self.cmc,self.color_indicator,self.colors,
        return [self.name,self.mana_cost,self.artist,self.cmc,self.color_indicator,self.colors,
                self.flavor_text,self.illustration_id,self.layout,self.loyalty,self.oracle_id,self.oracle_text,
                self.power,self.toughness,self.printed_name,self.printed_text,self.printed_type_line,
                self.type_line,self.watermark]

    def setPrintKey(self,print_key):
        self.print_key = print_key

    
    @staticmethod
    def addToBatch(scryfall_id,faces):
        face_data = []
        if faces is not None:
            for face in faces:
                face_data.append(face.getPersistData())
            CardFace._batch_data[scryfall_id] = face_data

    @staticmethod
    def getBatchData():
        batch_data = []
        for scryfall_id,data in CardFace._batch_data.items():
            print_key = MTGPrint.getPrintKey(scryfall_id)
            for face in data:
                batch_data.append([print_key] + face)
        CardFace._batch_data = {}
        return batch_data


class RelatedCard():
    insert_sql = "INSERT INTO RelatedCards(print_key,scryfall_id,component,name,type_line,uri) VALUES(%s,%s,%s,%s,%s,%s)"
    delete_sql_start = "DELETE FROM RelatedCards where print_key in ("

    _batch_data = {}

    def __init__(self,data):
        self.scryfall_id = data.get('id')
        self.component = data.get('component')
        self.name = data.get('name')
        self.type_line = data.get('type_line')
        self.uri = data.get('uri')

#        related_card_sql = "INSERT INTO RelatedCards(print_key,scryfall_id,component,name,type_line,uri) VALUES(%s,%s,%s,%s,%s,%s)"

    def getPersistData(self):
        #return [self.print_key,self.scryfall_id,self.component,self.name,self.type_line,self.uri]
        return [self.scryfall_id,self.component,self.name,self.type_line,self.uri]

    def setPrintKey(self,print_key):
        self.print_key = print_key

    @staticmethod
    def addToBatch(scryfall_id,parts):
        part_data = []
        if parts is not None:
            for part in parts:
                part_data.append(part.getPersistData())
            RelatedCard._batch_data[scryfall_id] = part_data

    @staticmethod
    def getBatchData():
        batch_data = []
        for scryfall_id,data in RelatedCard._batch_data.items():
            print_key = MTGPrint.getPrintKey(scryfall_id)
            for part in data:
                batch_data.append([print_key] + part)
        RelatedCard._batch_data = {}
        return batch_data

class MTGCard():
    existing_cards_sql = "SELECT c.id,c.name,ifnull(c.oracle_id,''),c.hash,c.update_time FROM Cards c"
    insert_sql = "INSERT INTO Cards(name,oracle_id,prints_search_uri,cmc,color_identity,reserved,type_line,oracle_text,color_indicator,color,edhc_rank,loyalty,mana_cost,penny_rank,power,toughness,produced_mana,hand_modifier,life_modifier,hash,update_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    id_sql_start = "SELECT id,name,ifnull(oracle_id,'') FROM Cards WHERE name in ("
    update_sql = "UPDATE Cards SET name=%s,oracle_id=%s,prints_search_uri=%s,cmc=%s,color_identity=%s,reserved=%s,type_line=%s,oracle_text=%s,color_indicator=%s,color=%s,edhc_rank=%s,loyalty=%s,mana_cost=%s,penny_rank=%s,power=%s,toughness=%s,produced_mana=%s,hand_modifier=%s,life_modifier=%s,hash=%s,update_count=update_count+1,update_time=%s WHERE id = %s"

    id_map = {}
    date_map = {}
    hashes = set()
    _result = DBConnection.singleQuery(existing_cards_sql)
    for c in _result:
        id_map[c[1]+c[2]]=c[0]
        hashes.add(c[3])
        date_map[c[1]+c[2]] = c[4]
    _result = None

    _new_cards = []
    _new_card_ids = set()
    _update_cards = []
    
    def __init__(self,data, data_date = datetime.now()):
        self.data_date = data_date

        # Read JSON data
        self.name = data.get('name')
        self.oracle_id = data.get('oracle_id')
        self.prints_search_uri = data.get('prints_search_uri')
        self.cmc = data.get('cmc')
        self.color_identity = data.get('color_identity')
        if self.color_identity is not None:
            self.color_identity = ''.join(self.color_identity)
        self.reserved = data.get('reserved')=="true"
        self.type_line = data.get('type_line')
        
        self.oracle_text = data.get('oracle_text')
        self.color_indicator = data.get('color_indicator')
        if self.color_indicator is not None:
            self.color_indicator = ''.join(self.color_indicator)
        self.colors = data.get('colors')
        if self.colors is not None:
            self.colors = ''.join(self.colors)
        self.edhrec_rank = data.get('edhrec_rank')
        self.loyalty = data.get('loyalty')
        self.mana_cost = data.get('mana_cost')
        self.penny_rank = data.get('penny_rank')
        self.power = data.get('power')
        self.produced_mana = data.get('produced_mana')
        if self.produced_mana is not None:
            self.produced_mana = ''.join(self.produced_mana)
        self.toughness = data.get('toughness')

        self.hand_modifier = data.get('hand_modifier')
        self.life_modifier = data.get('life_modifier')

        # card name is not a unique identifier by itself as there are certain tokens that have the same name but different card properties
        # oracle_id should be a unique identifer, except that reversible cards do not have an oracle_id. Instead each face will have its own oracle_id
        # however the combination of card name and oracle_id should provide a unique identifier for all cards
        self.card_id = ''.join(filter(None,[self.name,self.oracle_id]))

        # Calculate hash of card data so we can easily identify if existing data needs to be updated
        self.md5 = hashlib.md5(''.join(str(field) for field in self.getPersistData()).encode('utf-8')).hexdigest()

        if not self.exists() and self.card_id not in MTGCard._new_card_ids:
            MTGCard._new_cards.append(self)
            MTGCard._new_card_ids.add(self.card_id)
            MTGCard.hashes.add(self.md5)
        elif self.needsUpdate():
            MTGCard._update_cards.append(self)
            MTGCard.hashes.add(self.md5)

    def exists(self):
        return self.card_id in MTGCard.id_map
    
    def needsUpdate(self):
        return self.exists() and self.md5 not in MTGCard.hashes and self.data_date > MTGCard.date_map[self.card_id]

    def getName(self):
        return self.name
    
    def getOracleId(self):
        return self.oracle_id
    
    def getCardId(self):
        return self.card_id
    
    def getDate(self):
        return self.data_date
    
    def getMD5(self):
        #if self.md5 is None:
        #    self.md5 = hashlib.md5(''.join(str(field) for field in self.getPersistData()).encode('utf-8')).hexdigest()
        return self.md5
    
    def getPersistData(self):
        # name,oracle_id,prints_search_id,cmc_color_identity,reserved,type_line,oracle_text,color_indicator,color,edhc_rank,loyalty,mana_cost,penny_rank,power,toughness,produced_mana,hand_modifier,life_modifier
        return [self.name,self.oracle_id,self.prints_search_uri,self.cmc,self.color_identity,self.reserved,self.type_line,
                self.oracle_text,self.color_indicator,self.colors,self.edhrec_rank,self.loyalty,self.mana_cost,self.penny_rank,
                self.power,self.toughness,self.produced_mana,self.hand_modifier,self.life_modifier]

    def setId(self,id):
        self.id = id
    
    @staticmethod
    def getCardKey(card_id):
        return MTGCard.id_map.get(card_id)

    @staticmethod
    def hasNewData():
        return len(MTGCard._new_cards) > 0
    
    @staticmethod
    def hasUpdateData():
        return len(MTGCard._update_cards) > 0
    
    """ @staticmethod
    def getNewBatch():
        new_card_data = []

        if len(MTGCard._new_cards) > BATCH_SIZE:
            new_cards = MTGCard._new_cards[:BATCH_SIZE]
            MTGCard._new_cards = MTGCard._new_cards[BATCH_SIZE:]
        else:
            new_cards = MTGCard._new_cards
            MTGCard._new_cards = []

        for card in new_cards:
            #card_id = card.getCardId()
            #card_data = card.getPersistData() + [card.getMD5(),card.getDate()]

            new_card_data.append(card.getPersistData() + [card.getMD5(),card.getDate()])

        return new_card_data """
    
    @staticmethod
    def getNewBatch():
        return MTGCard.getBatch(MTGCard._new_cards)
    
    @staticmethod
    def getUpdateBatch():
        card_data = []
        for card in  MTGCard.getBatch(MTGCard._update_cards):
            card_id = ''.join(filter(None,[card[0],card[1]]))
            card_data.append(card + [MTGCard.getCardKey(card_id)])
        return card_data

    @staticmethod
    def getBatch(card_type):
        card_data = []
        batch_cards = card_type[:BATCH_SIZE]
        del card_type[:BATCH_SIZE]

        for card in batch_cards:
            card_data.append(card.getPersistData() + [card.getMD5(),card.getDate()])

        return card_data
        
    
    @staticmethod
    def updateIds(new_card_data,results):
        data = {}
        for nc in new_card_data:
            card_id = ''.join(filter(None,[nc[0],nc[1]]))
            data[card_id] = [nc[-2],nc[-1]]
        for c in results:
            card_id = ''.join(filter(None,[c[1],c[2]]))
            r = data.get(card_id)
            if r is not None:
                MTGCard.id_map[card_id] = c[0]
                #MTGCard.hashes.add(r[0])
                MTGCard.date_map[card_id] = r[1]




#    @staticmethod
#    def getExistingCardData():
#        card_id_map = {}
#        card_date_map = {}
#        card_hashes = set()
#        result = DBConnection.singleQuery(MTGCard.existing_cards_sql)
#        #cursor.execute(current_cards_sql)
#        for c in result:
#            card_id_map[c[1]+c[2]]=c[0]
#            card_hashes.add(c[3])
#            card_date_map[c[1]+c[2]] = c[4]
#        return card_id_map,card_date_map,card_hashes