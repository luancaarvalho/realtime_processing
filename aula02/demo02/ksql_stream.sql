SET 'auto.offset.reset' = 'earliest';

CREATE STREAM sales_raw (
  event_id VARCHAR,
  symbol VARCHAR,
  price DOUBLE,
  qty DOUBLE,
  event_time_ms BIGINT,
  sent_at_ms BIGINT
) WITH (
  KAFKA_TOPIC='rt.sales.raw',
  VALUE_FORMAT='JSON',
  TIMESTAMP='event_time_ms'
);
