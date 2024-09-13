ALTER TABLE `Cards` 
MODIFY `name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NOT NULL,
MODIFY `oracle_text` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
MODIFY `card_id` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci GENERATED ALWAYS AS (upper(concat(`name`,ifnull(`oracle_id`,'')))) VIRTUAL;

ALTER TABLE `RelatedCards`
MODIFY `name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NOT NULL;

ALTER TABLE `CardFaces`
MODIFY `name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NOT NULL;