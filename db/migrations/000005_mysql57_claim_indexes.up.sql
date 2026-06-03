ALTER TABLE `instances`
  ADD COLUMN `lock_token` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER `worker`,
  MODIFY COLUMN `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  MODIFY COLUMN `completed_at` DATETIME(6) NULL,
  MODIFY COLUMN `locked_until` DATETIME(6) NULL,
  MODIFY COLUMN `sticky_until` DATETIME(6) NULL;

ALTER TABLE `pending_events`
  MODIFY COLUMN `timestamp` DATETIME(6) NOT NULL,
  MODIFY COLUMN `visible_at` DATETIME(6) NULL;

ALTER TABLE `history`
  MODIFY COLUMN `timestamp` DATETIME(6) NOT NULL,
  MODIFY COLUMN `visible_at` DATETIME(6) NULL;

ALTER TABLE `activities`
  ADD COLUMN `lock_token` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER `worker`,
  MODIFY COLUMN `timestamp` DATETIME(6) NOT NULL,
  MODIFY COLUMN `visible_at` DATETIME(6) NULL,
  MODIFY COLUMN `locked_until` DATETIME(6) NULL;

DROP INDEX `idx_instances_locked_until_completed_at_queue` ON `instances`;
DROP INDEX `idx_activities_locked_until_queue` ON `activities`;

CREATE INDEX `idx_instances_claim` ON `instances` (`state`, `queue`, `completed_at`, `locked_until`, `sticky_until`, `worker`, `id`);
CREATE INDEX `idx_activities_claim` ON `activities` (`queue`, `locked_until`, `visible_at`, `id`);
CREATE INDEX `idx_activities_lease` ON `activities` (`instance_id`, `execution_id`, `activity_id`, `worker`, `queue`, `lock_token`);
