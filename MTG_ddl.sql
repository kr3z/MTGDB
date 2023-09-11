CREATE SEQUENCE `id_seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 100 cache 1000 nocycle ENGINE=InnoDB;

CREATE TABLE `AdditionalData` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `print_key` int(11) NOT NULL,
  `type` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `type2` varchar(200) COLLATE latin1_general_ci DEFAULT NULL,
  `value` varchar(600) COLLATE latin1_general_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_print_key` (`print_key`),
  KEY `ix_type` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `CardFaces` (
  `id` int(11) NOT NULL,
  `print_key` int(11) NOT NULL,
  `name` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `mana_cost` varchar(30) COLLATE latin1_general_ci NOT NULL,
  `artist` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `cmc` decimal(8,1) DEFAULT NULL,
  `color_indicator` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `colors` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `flavor_text` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `illustration_id` varchar(36) COLLATE latin1_general_ci DEFAULT NULL,
  `layout` varchar(30) COLLATE latin1_general_ci DEFAULT NULL,
  `oracle_id` varchar(36) COLLATE latin1_general_ci DEFAULT NULL,
  `oracle_text` varchar(1500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `power` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `toughness` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `printed_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `printed_text` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `printed_type_line` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `type_line` varchar(200) COLLATE latin1_general_ci DEFAULT NULL,
  `watermark` varchar(50) COLLATE latin1_general_ci DEFAULT NULL,
  `loyalty` varchar(50) COLLATE latin1_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_print_key` (`print_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `Cards` (
  `id` int(11) NOT NULL,
  `name` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `oracle_id` varchar(36) COLLATE latin1_general_ci DEFAULT NULL,
  `prints_search_uri` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `cmc` decimal(8,1) DEFAULT NULL,
  `color_identity` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `reserved` tinyint(1) NOT NULL,
  `type_line` varchar(100) COLLATE latin1_general_ci DEFAULT NULL,
  `oracle_text` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `color_indicator` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `color` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `edhc_rank` int(11) DEFAULT NULL,
  `loyalty` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `mana_cost` varchar(50) COLLATE latin1_general_ci DEFAULT NULL,
  `penny_rank` int(11) DEFAULT NULL,
  `power` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `toughness` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `produced_mana` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `hand_modifier` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `life_modifier` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `hash` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `update_count` int(11) DEFAULT '0',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `card_id` varchar(200) GENERATED ALWAYS AS (concat(`name`,ifnull(`oracle_id`,''))) VIRTUAL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `ux_name,oracle_id` (`name`,`oracle_id`),
  UNIQUE KEY `ux_Cards_card_id` (`card_id`),
  KEY `ix_oracle_id` (`oracle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `Collection` (
  `id` int(11) NOT NULL,
  `print_key` int(11) NOT NULL,
  `file_key` int(11) NOT NULL,
  `card_name` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `set_name` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `card_condition` varchar(30) COLLATE latin1_general_ci NOT NULL,
  `foil` tinyint(1) NOT NULL,
  `language` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `multiverse_id` int(11) DEFAULT NULL,
  `scryfall_id` varchar(36) COLLATE latin1_general_ci NOT NULL,
  `notes` text COLLATE latin1_general_ci,
  PRIMARY KEY (`id`),
  KEY `ix_print_key` (`print_key`),
  KEY `ix_file_key` (`file_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `ImportFiles` (
  `id` int(11) NOT NULL,
  `name` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `type` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `imported_at` datetime NOT NULL,
  `line_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `Legalities` (
  `id` int(11) NOT NULL,
  `print_key` int(11) NOT NULL,
  `standard` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `future` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `historic` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `gladiator` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `pioneer` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `explorer` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `modern` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `legacy` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `pauper` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `vintage` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `penny` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `commander` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `oathbreaker` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `brawl` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `historicbrawl` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `alchemy` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `paupercommander` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `duel` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `oldschool` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `premodern` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `predh` varchar(10) COLLATE latin1_general_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_print_key` (`print_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `Prices` (
  `id` int(11) NOT NULL,
  `print_key` int(11) NOT NULL,
  `price_date` date NOT NULL,
  `usd` decimal(9,2) DEFAULT NULL,
  `usd_foil` decimal(9,2) DEFAULT NULL,
  `usd_etched` decimal(9,2) DEFAULT NULL,
  `eur` decimal(9,2) DEFAULT NULL,
  `eur_foil` decimal(9,2) DEFAULT NULL,
  `tix` decimal(9,2) DEFAULT NULL,
  `is_latest` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `ix_print_key` (`print_key`)
  KEY `ix_Prices_price_date` (`price_date`),
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `Prints` (
  `id` int(11) NOT NULL,
  `card_key` int(11) DEFAULT NULL,
  `scryfall_id` varchar(36) NOT NULL,
  `set_key` int(11) NOT NULL,
  `lang` varchar(5) NOT NULL,
  `oversized` tinyint(1) NOT NULL,
  `layout` varchar(30) NOT NULL,
  `booster` tinyint(1) NOT NULL,
  `border_color` varchar(30) NOT NULL,
  `card_back_id` varchar(36) DEFAULT NULL,
  `collector_number` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `digital` tinyint(1) NOT NULL,
  `frame` varchar(10) NOT NULL,
  `full_art` tinyint(1) NOT NULL,
  `highres_image` tinyint(1) NOT NULL,
  `image_status` varchar(30) NOT NULL,
  `promo` tinyint(1) NOT NULL,
  `rarity` varchar(10) NOT NULL,
  `released_at` date NOT NULL,
  `reprint` tinyint(1) NOT NULL,
  `story_spotlight` tinyint(1) NOT NULL,
  `textless` tinyint(1) NOT NULL,
  `variation` tinyint(1) NOT NULL,
  `arena_id` int(11) DEFAULT NULL,
  `mtgo_id` int(11) DEFAULT NULL,
  `mtgo_foil_id` int(11) DEFAULT NULL,
  `tcgplayer_id` int(11) DEFAULT NULL,
  `tcgplayer_etched_id` int(11) DEFAULT NULL,
  `cardmarket_id` int(11) DEFAULT NULL,
  `artist` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `content_warning` tinyint(1) DEFAULT NULL,
  `flavor_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `illustration_id` varchar(36) DEFAULT NULL,
  `variation_of` varchar(36) DEFAULT NULL,
  `security_stamp` varchar(20) DEFAULT NULL,
  `watermark` varchar(30) DEFAULT NULL,
  `preview_previewed_at` date DEFAULT NULL,
  `finish_nonfoil` tinyint(1) NOT NULL,
  `finish_foil` tinyint(1) NOT NULL,
  `finish_etched` tinyint(1) NOT NULL,
  `game_paper` tinyint(1) NOT NULL,
  `game_mtgo` tinyint(1) NOT NULL,
  `game_arena` tinyint(1) NOT NULL,
  `game_astral` tinyint(1) NOT NULL,
  `game_sega` tinyint(1) NOT NULL,
  `hash` varchar(200) NOT NULL,
  `update_count` int(11) NOT NULL DEFAULT 0,
  `update_time` datetime NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_scryfall_id` (`scryfall_id`),
  KEY `ix_card_key` (`card_key`),
  KEY `ix_set_key` (`set_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `Prints_Additional` (
  `id` int(11) NOT NULL,
  `rulings_uri` varchar(100) NOT NULL,
  `scryfall_uri` varchar(500) NOT NULL,
  `uri` varchar(100) NOT NULL,
  `flavor_text` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `printed_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `printed_text` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `printed_type_line` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `preview_source_uri` varchar(200) DEFAULT NULL,
  `preview_source` varchar(200) DEFAULT NULL,
  `cardhoarder_purchase_uri` varchar(500) DEFAULT NULL,
  `cardmarket_purchase_uri` varchar(500) DEFAULT NULL,
  `tcgplayer_purchase_uri` varchar(500) DEFAULT NULL,
  `edhrec_uri` varchar(500) DEFAULT NULL,
  `gatherer_uri` varchar(200) DEFAULT NULL,
  `tcgplayer_infinite_articles_uri` varchar(500) DEFAULT NULL,
  `tcgplayer_infinite_decks_uri` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci `PAGE_COMPRESSED`=1;


CREATE TABLE `RelatedCards` (
  `id` int(11) NOT NULL,
  `print_key` int(11) NOT NULL,
  `scryfall_id` varchar(36) COLLATE latin1_general_ci NOT NULL,
  `component` varchar(50) COLLATE latin1_general_ci NOT NULL,
  `name` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `type_line` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `uri` varchar(200) COLLATE latin1_general_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_print_key` (`print_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `SetTypes` (
  `id` int(11) NOT NULL,
  `type` varchar(30) COLLATE latin1_general_ci NOT NULL,
  `description` varchar(200) COLLATE latin1_general_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE TABLE `Sets` (
  `id` int(11) NOT NULL,
  `scryfall_id` varchar(36) COLLATE latin1_general_ci NOT NULL,
  `code` varchar(10) COLLATE latin1_general_ci NOT NULL,
  `name` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `SetType_key` int(11) NOT NULL,
  `card_count` smallint(6) NOT NULL,
  `digital` tinyint(1) NOT NULL,
  `foil_only` tinyint(1) NOT NULL,
  `nonfoil_only` tinyint(1) NOT NULL,
  `scryfall_uri` varchar(500) COLLATE latin1_general_ci NOT NULL,
  `uri` varchar(500) COLLATE latin1_general_ci NOT NULL,
  `icon_svg_uri` varchar(500) COLLATE latin1_general_ci NOT NULL,
  `search_uri` varchar(500) COLLATE latin1_general_ci NOT NULL,
  `mtgo_code` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `tcgplayer_id` int(11) DEFAULT NULL,
  `released_at` date DEFAULT NULL,
  `block_code` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `block` varchar(50) COLLATE latin1_general_ci DEFAULT NULL,
  `parent_set_code` varchar(10) COLLATE latin1_general_ci DEFAULT NULL,
  `printed_size` smallint(6) DEFAULT NULL,
  `hash` varchar(200) COLLATE latin1_general_ci NOT NULL,
  `update_count` int(11) NOT NULL DEFAULT '0',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_scryfall_id` (`scryfall_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

INSERT INTO `SetTypes` VALUES
(1,'core','A yearly Magic core set (Tenth Edition, etc)'),
(2,'expansion','A rotational expansion set in a block (Zendikar, etc)'),
(3,'masters','A reprint set that contains no new cards (Modern Masters, etc)'),
(4,'alchemy','An Arena set designed for Alchemy'),
(5,'masterpiece','Masterpiece Series premium foil cards'),
(6,'arsenal','A Commander-oriented gift set'),
(7,'from_the_vault','From the Vault gift sets'),
(8,'spellbook','Spellbook series gift sets'),
(9,'premium_deck','Premium Deck Series decks'),
(10,'duel_deck','Duel Decks'),
(11,'draft_innovation','Special draft sets, like Conspiracy and Battlebond'),
(12,'treasure_chest','Magic Online treasure chest prize sets'),
(13,'commander','Commander preconstructed decks'),
(14,'planechase','Planechase sets'),
(15,'archenemy','Archenemy sets'),
(16,'vanguard','Vanguard card sets'),
(17,'funny','A funny un-set or set with funny promos (Unglued, Happy Holidays, etc)'),
(18,'starter','A starter/introductory set (Portal, etc)'),
(19,'box','A gift box set'),
(20,'promo','A set that contains purely promotional cards'),
(21,'token','A set made up of tokens and emblems.'),
(22,'memorabilia','A set made up of gold-bordered, oversize, or trophy cards that are not legal'),
(23,'minigame','A set that contains minigame card inserts from booster packs');
COMMIT;