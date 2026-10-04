ALTER TABLE reader_memory_operations
 ADD COLUMN evaluation_context_json MEDIUMTEXT NULL,
 ADD COLUMN evaluation_context_expires_at DATETIME(6) NULL,
 ADD KEY reader_memory_evaluation_retention (evaluation_context_expires_at),
 ADD CONSTRAINT reader_memory_evaluation_context_json CHECK (evaluation_context_json IS NULL OR JSON_VALID(evaluation_context_json)),
 ADD CONSTRAINT reader_memory_evaluation_context_size CHECK (evaluation_context_json IS NULL OR OCTET_LENGTH(evaluation_context_json)<=1048576),
 ALGORITHM=COPY, LOCK=SHARED