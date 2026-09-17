ALTER TABLE reader_memory_operations
 ADD COLUMN batch_uuid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
 ADD KEY reader_memory_batch (batch_uuid,status)
