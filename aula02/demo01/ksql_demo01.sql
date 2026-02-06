SET 'auto.offset.reset' = 'earliest';

CREATE STREAM files_raw (
  file_id VARCHAR,
  user_id VARCHAR,
  `size` INT,
  event_time_ms BIGINT,
  sent_at_ms BIGINT
) WITH (
  KAFKA_TOPIC='rt.files.raw',
  VALUE_FORMAT='JSON',
  TIMESTAMP='event_time_ms'
);

CREATE TABLE files_per_min AS
  SELECT
    user_id,
    WINDOWSTART AS window_start_ms,
    WINDOWEND AS window_end_ms,
    COUNT(*) AS file_count,
    SUM(`size`) AS total_size
  FROM files_raw
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY user_id
  EMIT CHANGES;

SELECT * FROM files_per_min EMIT CHANGES;
