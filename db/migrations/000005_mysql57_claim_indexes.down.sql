DROP INDEX `idx_activities_lease` ON `activities`;
DROP INDEX `idx_activities_claim` ON `activities`;
DROP INDEX `idx_instances_claim` ON `instances`;

CREATE INDEX `idx_instances_locked_until_completed_at_queue` ON `instances` (`completed_at`, `locked_until`, `sticky_until`, `worker`, `queue`);
CREATE INDEX `idx_activities_locked_until_queue` ON `activities` (`locked_until`, `queue`);

ALTER TABLE `activities`
  MODIFY COLUMN `timestamp` DATETIME NOT NULL,
  MODIFY COLUMN `visible_at` DATETIME NULL,
  MODIFY COLUMN `locked_until` DATETIME NULL,
  DROP COLUMN `lock_token`;

ALTER TABLE `history`
  MODIFY COLUMN `timestamp` DATETIME NOT NULL,
  MODIFY COLUMN `visible_at` DATETIME NULL;

ALTER TABLE `pending_events`
  MODIFY COLUMN `timestamp` DATETIME NOT NULL,
  MODIFY COLUMN `visible_at` DATETIME NULL;

ALTER TABLE `instances`
  MODIFY COLUMN `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY COLUMN `completed_at` DATETIME NULL,
  MODIFY COLUMN `locked_until` DATETIME NULL,
  MODIFY COLUMN `sticky_until` DATETIME NULL,
  DROP COLUMN `lock_token`;
