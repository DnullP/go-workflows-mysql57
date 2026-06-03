CREATE TABLE `activities` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `activity_id` varchar(64) CHARACTER SET utf8 NOT NULL,
  `instance_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `execution_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `event_type` int(11) NOT NULL,
  `timestamp` datetime(6) NOT NULL,
  `schedule_event_id` bigint(20) NOT NULL,
  `visible_at` datetime(6) DEFAULT NULL,
  `locked_until` datetime(6) DEFAULT NULL,
  `worker` varchar(64) CHARACTER SET utf8 DEFAULT NULL,
  `lock_token` char(36) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `queue` varchar(128) CHARACTER SET utf8 DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_activities_instance_id_execution_id_activity_id_worker` (`instance_id`,`execution_id`,`activity_id`,`worker`),
  KEY `idx_activities_claim` (`queue`,`locked_until`,`visible_at`,`id`),
  KEY `idx_activities_lease` (`instance_id`,`execution_id`,`activity_id`,`worker`,`queue`,`lock_token`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `attributes` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `instance_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `execution_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `data` mediumblob NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_attributes_instance_id_execution_id_event_id` (`instance_id`,`execution_id`,`event_id`),
  KEY `idx_attributes_event_id` (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `history` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` varchar(64) CHARACTER SET utf8 NOT NULL,
  `sequence_id` bigint(20) NOT NULL,
  `instance_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `execution_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `event_type` int(11) NOT NULL,
  `timestamp` datetime(6) NOT NULL,
  `schedule_event_id` bigint(20) NOT NULL,
  `visible_at` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_history_instance_id_execution_id` (`instance_id`,`execution_id`),
  KEY `idx_history_instance_id_execution_id_sequence_id` (`instance_id`,`execution_id`,`sequence_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `instances` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `instance_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `execution_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `parent_instance_id` varchar(128) CHARACTER SET utf8 DEFAULT NULL,
  `parent_execution_id` varchar(128) CHARACTER SET utf8 DEFAULT NULL,
  `parent_schedule_event_id` bigint(20) DEFAULT NULL,
  `metadata` blob,
  `state` int(11) NOT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `completed_at` datetime(6) DEFAULT NULL,
  `locked_until` datetime(6) DEFAULT NULL,
  `sticky_until` datetime(6) DEFAULT NULL,
  `worker` varchar(64) CHARACTER SET utf8 DEFAULT NULL,
  `lock_token` char(36) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  `queue` varchar(128) CHARACTER SET utf8 DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_instances_instance_id_execution_id` (`instance_id`,`execution_id`),
  KEY `idx_instances_parent_instance_id_parent_execution_id` (`parent_instance_id`,`parent_execution_id`),
  KEY `idx_instances_claim` (`state`,`queue`,`completed_at`,`locked_until`,`sticky_until`,`worker`,`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `pending_events` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `sequence_id` bigint(20) NOT NULL,
  `instance_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `execution_id` varchar(128) CHARACTER SET utf8 NOT NULL,
  `event_type` int(11) NOT NULL,
  `timestamp` datetime(6) NOT NULL,
  `schedule_event_id` bigint(20) NOT NULL,
  `visible_at` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_pending_events_inid_exid` (`instance_id`,`execution_id`),
  KEY `idx_pending_events_inid_exid_visible_at_schedule_event_id` (`instance_id`,`execution_id`,`visible_at`,`schedule_event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `schema_migrations` (
  `version` bigint(20) NOT NULL,
  `dirty` tinyint(1) NOT NULL,
  PRIMARY KEY (`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `schema_migrations` VALUES (5,0);
