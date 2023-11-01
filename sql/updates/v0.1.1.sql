UPDATE Legalities set 
standard=CASE WHEN standard='legal' THEN 1 WHEN standard='not_legal' THEN 2 WHEN standard='restricted' THEN 3 WHEN standard='banned' THEN 4 END,
future=CASE WHEN future='legal' THEN 1 WHEN future='not_legal' THEN 2 WHEN future='restricted' THEN 3 WHEN future='banned' THEN 4 END,
historic=CASE WHEN historic='legal' THEN 1 WHEN historic='not_legal' THEN 2 WHEN historic='restricted' THEN 3 WHEN historic='banned' THEN 4 END,
gladiator=CASE WHEN gladiator='legal' THEN 1 WHEN gladiator='not_legal' THEN 2 WHEN gladiator='restricted' THEN 3 WHEN gladiator='banned' THEN 4 END,
pioneer=CASE WHEN pioneer='legal' THEN 1 WHEN pioneer='not_legal' THEN 2 WHEN pioneer='restricted' THEN 3 WHEN pioneer='banned' THEN 4 END,
explorer=CASE WHEN explorer='legal' THEN 1 WHEN explorer='not_legal' THEN 2 WHEN explorer='restricted' THEN 3 WHEN explorer='banned' THEN 4 END,
modern=CASE WHEN modern='legal' THEN 1 WHEN modern='not_legal' THEN 2 WHEN modern='restricted' THEN 3 WHEN modern='banned' THEN 4 END,
legacy=CASE WHEN legacy='legal' THEN 1 WHEN legacy='not_legal' THEN 2 WHEN legacy='restricted' THEN 3 WHEN legacy='banned' THEN 4 END,
pauper=CASE WHEN pauper='legal' THEN 1 WHEN pauper='not_legal' THEN 2 WHEN pauper='restricted' THEN 3 WHEN pauper='banned' THEN 4 END,
vintage=CASE WHEN vintage='legal' THEN 1 WHEN vintage='not_legal' THEN 2 WHEN vintage='restricted' THEN 3 WHEN vintage='banned' THEN 4 END,
penny=CASE WHEN penny='legal' THEN 1 WHEN penny='not_legal' THEN 2 WHEN penny='restricted' THEN 3 WHEN penny='banned' THEN 4 END,
commander=CASE WHEN commander='legal' THEN 1 WHEN commander='not_legal' THEN 2 WHEN commander='restricted' THEN 3 WHEN commander='banned' THEN 4 END,
oathbreaker=CASE WHEN oathbreaker='legal' THEN 1 WHEN oathbreaker='not_legal' THEN 2 WHEN oathbreaker='restricted' THEN 3 WHEN oathbreaker='banned' THEN 4 END,
brawl=CASE WHEN brawl='legal' THEN 1 WHEN brawl='not_legal' THEN 2 WHEN brawl='restricted' THEN 3 WHEN brawl='banned' THEN 4 END,
historicbrawl=CASE WHEN historicbrawl='legal' THEN 1 WHEN historicbrawl='not_legal' THEN 2 WHEN historicbrawl='restricted' THEN 3 WHEN historicbrawl='banned' THEN 4 END,
alchemy=CASE WHEN alchemy='legal' THEN 1 WHEN alchemy='not_legal' THEN 2 WHEN alchemy='restricted' THEN 3 WHEN alchemy='banned' THEN 4 END,
paupercommander=CASE WHEN paupercommander='legal' THEN 1 WHEN paupercommander='not_legal' THEN 2 WHEN paupercommander='restricted' THEN 3 WHEN paupercommander='banned' THEN 4 END,
duel=CASE WHEN duel='legal' THEN 1 WHEN duel='not_legal' THEN 2 WHEN duel='restricted' THEN 3 WHEN duel='banned' THEN 4 END,
oldschool=CASE WHEN oldschool='legal' THEN 1 WHEN oldschool='not_legal' THEN 2 WHEN oldschool='restricted' THEN 3 WHEN oldschool='banned' THEN 4 END,
premodern=CASE WHEN premodern='legal' THEN 1 WHEN premodern='not_legal' THEN 2 WHEN premodern='restricted' THEN 3 WHEN premodern='banned' THEN 4 END,
predh=CASE WHEN predh='legal' THEN 1 WHEN predh='not_legal' THEN 2 WHEN predh='restricted' THEN 3 WHEN predh='banned' THEN 4 END;

ALTER TABLE Legalities
MODIFY standard tinyint NOT NULL,
MODIFY future tinyint NOT NULL,
MODIFY historic tinyint NOT NULL,
MODIFY gladiator tinyint NOT NULL,
MODIFY pioneer tinyint NOT NULL,
MODIFY explorer tinyint NOT NULL,
MODIFY modern tinyint NOT NULL,
MODIFY legacy tinyint NOT NULL,
MODIFY pauper tinyint NOT NULL,
MODIFY vintage tinyint NOT NULL,
MODIFY penny tinyint NOT NULL,
MODIFY commander tinyint NOT NULL,
MODIFY oathbreaker tinyint NOT NULL,
MODIFY brawl tinyint NOT NULL,
MODIFY historicbrawl tinyint NOT NULL,
MODIFY alchemy tinyint NOT NULL,
MODIFY paupercommander tinyint NOT NULL,
MODIFY duel tinyint NOT NULL,
MODIFY oldschool tinyint NOT NULL,
MODIFY premodern tinyint NOT NULL,
MODIFY predh tinyint NOT NULL,
ADD hash varchar(200) COLLATE latin1_general_ci,
ADD update_count int DEFAULT 0 NOT NULL,
ADD update_time datetime DEFAULT current_timestamp() NOT NULL,
COMMENT='legal = 1, not_legal = 2, restricted = 3, banned = 4';

ALTER TABLE RelatedCards
ADD `hash` varchar(200) COLLATE latin1_general_ci,
ADD `update_count` int(11) DEFAULT 0 NOT NULL,
ADD `update_time` datetime DEFAULT current_timestamp() NOT NULL;

ALTER TABLE Prints
ADD `multiverse_id`	int(11) AFTER `game_sega`,
ADD `multiverse_id_2`	int(11) AFTER `multiverse_id`;

ALTER TABLE SetTypes MODIFY `id` int(11) NOT NULL;

UPDATE Prints p
INNER JOIN(
    WITH multiverse_ids as (
    SELECT ad.print_key,ad.value,ad2.value as value2
    FROM AdditionalData ad
    INNER JOIN (
        SELECT print_key,min(id) as id
        FROM AdditionalData 
        WHERE type='multiverse_ids'
        GROUP BY print_key) low on low.id = ad.id
    left outer join AdditionalData ad2 on ad2.print_key=ad.print_key and low.id<ad2.id and ad2.type='multiverse_ids'
    WHERE ad.type = 'multiverse_ids') 
    SELECT * FROM multiverse_ids) m on p.id = m.print_key
SET p.multiverse_id=m.value,p.multiverse_id_2=m.value2
WHERE p.multiverse_id is null and p.multiverse_id_2 is null;

CREATE TABLE `PrintAttributes` (
  `id` int(11) NOT NULL,
  `attribute_type` int(11) NOT NULL COMMENT '''keyword'': 1, ''promo_type'': 2, ''frame_effects'': 3, ''attraction_lights'': 4',
  `attribute` varchar(200) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_PrintAttributes_attribute_type` (`attribute_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `PrintAttributeLink` (
  `id` int(11) NOT NULL,
  `attribute_key` int(11) NOT NULL,
  `print_key` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_attrlink_attrkey` (`attribute_key`),
  KEY `ix_attrlink_printkey` (`print_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;
