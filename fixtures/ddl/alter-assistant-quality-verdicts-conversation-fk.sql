ALTER TABLE `assistant_quality_verdicts`
    ADD CONSTRAINT `fk_aqv_conversation` FOREIGN KEY (`conversation_id`)
        REFERENCES `llm_conversations` (`id`) ON DELETE CASCADE