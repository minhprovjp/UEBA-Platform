-- ============================================================
-- 1. CẤU HÌNH DATABASE & CÁC BẢNG PHỤ TRỢ
-- ============================================================
DROP DATABASE IF EXISTS uba_db;
CREATE DATABASE IF NOT EXISTS uba_db CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

DROP USER IF EXISTS 'uba_user'@'localhost'
-- Tạo user uba_user nếu chưa có
CREATE USER IF NOT EXISTS 'uba_user'@'%' IDENTIFIED BY 'password';
-- Cấp quyền đọc bảng log cho uba_user
-- GRANT SELECT ON uba_db.* TO 'uba_user'@'localhost';
FLUSH PRIVILEGES;
GRANT CREATE USER ON *.* TO 'uba_user'@'localhost';
GRANT SELECT ON mysql.user TO 'uba_user'@'localhost';
GRANT PROCESS ON *.* TO 'uba_user'@'localhost';
GRANT CONNECTION_ADMIN ON *.* TO 'uba_user'@'localhost';
GRANT RELOAD ON *.* TO 'uba_user'@'localhost';

-- Áp dụng quyền ngay lập tức
FLUSH PRIVILEGES;

USE uba_db;

-- 1.1. Bảng trạng thái Server (Neo thời gian khởi động - Boot Anchor)
DROP TABLE IF EXISTS uba_server_state;
CREATE TABLE uba_server_state (
    id TINYINT UNSIGNED PRIMARY KEY,
    boot_time_anchor DATETIME(6) NOT NULL,
    last_uptime BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Khởi tạo giá trị mặc định
INSERT INTO uba_server_state (id, boot_time_anchor, last_uptime)
VALUES (1, UTC_TIMESTAMP(6), 0)
ON DUPLICATE KEY UPDATE id = 1;

-- 1.2. Bảng Metrics (Theo dõi hiệu năng của tiến trình Ingest)
DROP TABLE IF EXISTS uba_ingest_metrics;
CREATE TABLE uba_ingest_metrics (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    duration_ms INT UNSIGNED,
    rows_inserted INT UNSIGNED,
    rows_ignored INT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


-- ============================================================
-- 2. BẢNG LOG VẬT LÝ (PARTITIONED)
-- ============================================================
DROP TABLE IF EXISTS uba_persistent_log;
CREATE TABLE uba_persistent_log (
    id BIGINT UNSIGNED AUTO_INCREMENT,
    
    -- Partition Key & Time (Đặt đầu để tối ưu Partition Pruning)
    event_ts DATETIME(6) NOT NULL,
    
    -- Identity & Fingerprint
    event_id BIGINT UNSIGNED,
    thread_id BIGINT UNSIGNED,
    timer_start BIGINT UNSIGNED,
    server_boot_time DATETIME(6) NOT NULL,
    fingerprint CHAR(64) NOT NULL,

    -- Context
    processlist_user VARCHAR(64),
    processlist_host VARCHAR(255),
    connection_type VARCHAR(32),
    thread_os_id BIGINT UNSIGNED,
    current_schema VARCHAR(64),
    cpu_time BIGINT UNSIGNED,
    program_name VARCHAR(128),
    connector_name VARCHAR(128),
    client_os VARCHAR(128),
    source_host VARCHAR(128),
    
    
    -- Content
    sql_text LONGTEXT,
    digest VARCHAR(64),
    digest_text LONGTEXT,
    event_name VARCHAR(128),

    -- Metrics
    timer_wait BIGINT UNSIGNED,
    timer_end BIGINT UNSIGNED, 
    lock_time BIGINT UNSIGNED, -- Vẫn giữ để mapping 1:1 với Schema MySQL
    rows_sent BIGINT UNSIGNED,
    rows_examined BIGINT UNSIGNED,
    rows_affected BIGINT UNSIGNED,
    
    -- Optimizer & Errors
    mysql_errno INT,
    message_text TEXT,
    errors BIGINT UNSIGNED,
    warnings BIGINT UNSIGNED,
    created_tmp_disk_tables BIGINT UNSIGNED,
    created_tmp_tables BIGINT UNSIGNED,
    select_full_join BIGINT UNSIGNED,
    select_scan BIGINT UNSIGNED,
    sort_merge_passes BIGINT UNSIGNED,
    no_index_used BIGINT UNSIGNED,
    no_good_index_used BIGINT UNSIGNED,

    captured_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Keys & Indexes
    -- PK bắt buộc phải chứa event_ts vì dùng Partitioning
    PRIMARY KEY (id, event_ts),
    
    -- Unique Key để chống trùng lặp (Idempotency)
    UNIQUE KEY uniq_fp (fingerprint, event_ts),
    
    -- Index hỗ trợ query
    INDEX idx_event_ts (event_ts),
    INDEX idx_user_ts (processlist_user, event_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
-- Sử dụng RANGE COLUMNS (MySQL 8.0+)
PARTITION BY RANGE COLUMNS(event_ts) (
    PARTITION p_init VALUES LESS THAN ('2025-01-01 00:00:00'),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);


-- ============================================================
-- 3. PROCEDURES QUẢN LÝ PARTITION
-- ============================================================
DROP PROCEDURE IF EXISTS proc_add_daily_partition;
DELIMITER $$
CREATE PROCEDURE proc_add_daily_partition(IN p_day DATE)
BEGIN
    DECLARE part_name VARCHAR(32);
    DECLARE next_day DATE;
    DECLARE cnt INT;
    
    SET part_name = CONCAT('p', DATE_FORMAT(p_day, '%Y%m%d'));
    SET next_day = DATE_ADD(p_day, INTERVAL 1 DAY);
    
    SELECT COUNT(*) INTO cnt FROM information_schema.partitions 
    WHERE table_schema = DATABASE() AND table_name = 'uba_persistent_log' AND partition_name = part_name;
    
    IF cnt = 0 THEN
        -- Cú pháp cho RANGE COLUMNS: VALUES LESS THAN ('YYYY-MM-DD HH:MM:SS')
        SET @s = CONCAT('ALTER TABLE uba_persistent_log REORGANIZE PARTITION pmax INTO (',
                        'PARTITION ', part_name, ' VALUES LESS THAN (''', next_day, ' 00:00:00''),',
                        'PARTITION pmax VALUES LESS THAN (MAXVALUE))');
        PREPARE stmt FROM @s;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS proc_drop_partition_by_date;
DELIMITER $$
CREATE PROCEDURE proc_drop_partition_by_date(IN p_cutoff DATE)
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE pname VARCHAR(64);
    DECLARE cur CURSOR FOR 
        SELECT PARTITION_NAME FROM information_schema.partitions 
        WHERE table_schema = DATABASE() AND table_name = 'uba_persistent_log'
        AND partition_name NOT IN ('pmax', 'p_init');
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO pname;
        IF done THEN LEAVE read_loop; END IF;
        
        IF CHAR_LENGTH(pname) = 9 AND SUBSTRING(pname,1,1) = 'p' THEN
            SET @pdate = STR_TO_DATE(SUBSTRING(pname, 2), '%Y%m%d');
            IF @pdate < p_cutoff THEN
                SET @drop = CONCAT('ALTER TABLE uba_persistent_log DROP PARTITION ', pname);
                PREPARE stmt FROM @drop;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END IF;
        END IF;
    END LOOP;
    CLOSE cur;
END$$
DELIMITER ;


-- ============================================================
-- 4. EVENTS (SCHEDULER)
-- ============================================================
SET GLOBAL event_scheduler = ON;

DELIMITER $$

-- Xóa event cũ
DROP EVENT IF EXISTS flush_perf_schema_to_disk$$

CREATE EVENT flush_perf_schema_to_disk
ON SCHEDULE EVERY 2 SECOND
DO
BEGIN
    DECLARE v_current_uptime BIGINT UNSIGNED;
    DECLARE v_last_uptime BIGINT UNSIGNED;
    DECLARE v_boot_anchor DATETIME(6);
    DECLARE v_max_ts DATETIME(6);
    DECLARE v_start_time TIMESTAMP(6);
    DECLARE v_rows_before BIGINT;
    DECLARE v_rows_after BIGINT;

    SET v_start_time = CURRENT_TIMESTAMP(6);

    -- 1. Quản lý trạng thái Server (Detect Restart)
    -- Gắn tag AND 'UBA_EVENT' = 'UBA_EVENT' vào cuối mỗi lệnh
    SELECT VARIABLE_VALUE INTO v_current_uptime 
    FROM performance_schema.global_status 
    WHERE VARIABLE_NAME = 'UPTIME' AND 'UBA_EVENT' = 'UBA_EVENT';

    SELECT boot_time_anchor, last_uptime 
    INTO v_boot_anchor, v_last_uptime 
    FROM uba_server_state WHERE id = 1 AND 'UBA_EVENT' = 'UBA_EVENT';

    IF v_current_uptime < v_last_uptime 
       OR ABS(TIMESTAMPDIFF(
                SECOND, 
                DATE_SUB(UTC_TIMESTAMP(6), INTERVAL v_current_uptime SECOND), 
                v_boot_anchor)) > 60 
    THEN
        SET v_boot_anchor = DATE_SUB(UTC_TIMESTAMP(6), INTERVAL v_current_uptime SECOND);
    END IF;

    UPDATE uba_server_state 
    SET boot_time_anchor = v_boot_anchor, last_uptime = v_current_uptime 
    WHERE id = 1 AND 'UBA_EVENT' = 'UBA_EVENT';

    -- 2. Max timestamp
    SELECT COALESCE(MAX(event_ts), '1970-01-01 00:00:00') 
    INTO v_max_ts 
    FROM uba_persistent_log WHERE 'UBA_EVENT' = 'UBA_EVENT';

    SELECT COUNT(*) INTO v_rows_before
    FROM uba_persistent_log
    WHERE event_ts > (v_max_ts - INTERVAL 10 SECOND) AND 'UBA_EVENT' = 'UBA_EVENT';

    -- 3. Insert rows
    INSERT IGNORE INTO uba_persistent_log (
        event_id, thread_id, timer_start, server_boot_time, event_ts, fingerprint,
        processlist_user, processlist_host, connection_type, thread_os_id,
        current_schema, cpu_time, program_name, connector_name, client_os, source_host,
        sql_text, digest, digest_text, event_name,
        timer_wait, timer_end, lock_time, rows_sent, rows_examined, rows_affected,
        mysql_errno, message_text, errors, warnings,
        created_tmp_disk_tables, created_tmp_tables, select_full_join,
        select_scan, sort_merge_passes, no_index_used, no_good_index_used
    )
    SELECT
        e.EVENT_ID,
        e.THREAD_ID,
        e.TIMER_START,
        v_boot_anchor,
        DATE_ADD(v_boot_anchor, INTERVAL (e.TIMER_START DIV 1000000) MICROSECOND),
        SHA2(CONCAT(v_boot_anchor, '|', e.THREAD_ID, '|', e.EVENT_ID, '|', e.TIMER_START), 256),
        t.PROCESSLIST_USER, t.PROCESSLIST_HOST, t.CONNECTION_TYPE, t.THREAD_OS_ID,
        e.CURRENT_SCHEMA,
        e.CPU_TIME,
        (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs 
         WHERE PROCESSLIST_ID = t.PROCESSLIST_ID AND ATTR_NAME='program_name' LIMIT 1),
        (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs
         WHERE PROCESSLIST_ID = t.PROCESSLIST_ID AND ATTR_NAME='_connector_name' LIMIT 1),
        (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs
         WHERE PROCESSLIST_ID = t.PROCESSLIST_ID AND ATTR_NAME='_os' LIMIT 1),
        (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs
         WHERE PROCESSLIST_ID = t.PROCESSLIST_ID AND ATTR_NAME='_source_host' LIMIT 1),

        e.SQL_TEXT, e.DIGEST, e.DIGEST_TEXT, e.EVENT_NAME,
        e.TIMER_WAIT, e.TIMER_END, e.LOCK_TIME,
        e.ROWS_SENT, e.ROWS_EXAMINED, e.ROWS_AFFECTED,
        e.MYSQL_ERRNO, e.MESSAGE_TEXT, e.ERRORS, e.WARNINGS,
        e.CREATED_TMP_DISK_TABLES, e.CREATED_TMP_TABLES, e.SELECT_FULL_JOIN,
        e.SELECT_SCAN, e.SORT_MERGE_PASSES, e.NO_INDEX_USED, e.NO_GOOD_INDEX_USED
    FROM performance_schema.events_statements_history_long e
    LEFT JOIN performance_schema.threads t ON e.THREAD_ID = t.THREAD_ID
    WHERE e.SQL_TEXT IS NOT NULL
      AND e.SQL_TEXT NOT LIKE '%UBA_EVENT%'    -- tránh tự bắt chính mình
      AND e.SQL_TEXT NOT LIKE '%performance_schema%'
      AND e.SQL_TEXT NOT LIKE '%uba_persistent_log%'
      AND e.SQL_TEXT NOT LIKE '%uba_db%'
	  AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER NOT IN ('uba_user'))
      AND (e.CURRENT_SCHEMA IS NULL OR e.CURRENT_SCHEMA != 'uba_db')
      
--       AND e.SQL_TEXT NOT LIKE '%rollback%'
--       AND e.SQL_TEXT != 'FLUSH PRIVILEGES'
--       AND e.SQL_TEXT != '%version_comment%'
--       AND e.SQL_TEXT != '%auto_commit%'
--       AND e.EVENT_NAME != 'statement/sql/rollback'
--      AND e.EVENT_NAME NOT IN ('statement/sql/rollback', 'statement/sql/commit', 'statement/sql/set_option', 'statement/sql/xa_rollback')
--       AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER != 'uba_user')
      AND DATE_ADD(v_boot_anchor, INTERVAL (e.TIMER_START DIV 1000000) MICROSECOND) 
            > (v_max_ts - INTERVAL 10 SECOND)
      AND 'UBA_EVENT' = 'UBA_EVENT';
      
    -- 4. Ghi Metrics
    SELECT COUNT(*) INTO v_rows_after 
    FROM uba_persistent_log 
    WHERE event_ts > (v_max_ts - INTERVAL 10 SECOND) AND 'UBA_EVENT' = 'UBA_EVENT';

    INSERT INTO uba_ingest_metrics (duration_ms, rows_inserted, rows_ignored) 
	SELECT 
		TIMESTAMPDIFF(MICROSECOND, v_start_time, CURRENT_TIMESTAMP(6)) / 1000,
		(v_rows_after - v_rows_before), 
		0 
	FROM DUAL 
	WHERE 'UBA_EVENT' = 'UBA_EVENT';

END$$
DELIMITER ;


-- 4.2. Event Maintenance
DROP EVENT IF EXISTS partition_maintenance;
DELIMITER $$
CREATE EVENT partition_maintenance
ON SCHEDULE EVERY 1 DAY
STARTS (UTC_DATE() + INTERVAL 1 DAY + INTERVAL 1 HOUR)
DO
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE pdate DATE;

    -- Tạo trước partition cho 3 ngày tới
    WHILE i < 3 AND 'UBA_EVENT' = 'UBA_EVENT' DO
        SET pdate = DATE_ADD(UTC_DATE(), INTERVAL i DAY);
        CALL proc_add_daily_partition(pdate);
        SET i = i + 1;
    END WHILE;
    
    CALL proc_drop_partition_by_date(DATE_SUB(UTC_DATE(), INTERVAL 30 DAY)) /*+ UBA_EVENT */;
END$$
DELIMITER ;
