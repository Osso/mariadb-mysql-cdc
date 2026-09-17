ALTER TABLE reader_memory_profiles
 ADD COLUMN checkpoints_json TEXT NOT NULL DEFAULT '{}',
 ADD CONSTRAINT reader_memory_checkpoints_json CHECK (JSON_VALID(checkpoints_json)),
 ADD CONSTRAINT reader_memory_checkpoints_size CHECK (OCTET_LENGTH(checkpoints_json)<=16384)
