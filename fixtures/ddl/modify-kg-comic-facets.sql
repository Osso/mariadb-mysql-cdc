/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */ -- Self-healing for environments that already applied the VARCHAR(80) version of this file: MODIFY
-- to the same type is a no-op on a fresh CREATE, and widens an existing column in place. Widening
-- never truncates, so this is safe to re-run.
ALTER TABLE `kg_comic_facets`
    MODIFY COLUMN `facet_vocab_version` VARCHAR(128) NOT NULL
