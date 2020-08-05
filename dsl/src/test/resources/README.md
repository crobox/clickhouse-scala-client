# Clickhouse Test Script 

A script to easily test and verify queries matching Scala Tests.
First, connect to your local Clickhouse Server (using the native Clickhouse Client)

```
docker run  -it --rm --net=host yandex/clickhouse-client clickhouse-client -m
```

Then, create the tables by running the following commands:

```
CREATE DATABASE IF NOT EXISTS sc;

CREATE TABLE IF NOT EXISTS sc.captainAmerica  (
  shield_id String,
  ts UInt64,
  numbers Array(UInt32)
) ENGINE = Memory;

CREATE TABLE IF NOT EXISTS sc.twoTestTable (
  item_id String,
  column_1 String,
  column_2 UInt32,
  column_3 String,
  column_4 String
) ENGINE = Memory;

CREATE TABLE IF NOT EXISTS sc.threeTestTable (
  item_id String,
  column_4 String,
  column_5 String,
  column_6 String
) ENGINE = Memory;
```
If you want to create persistent tables...
```
CREATE DATABASE IF NOT EXISTS sc;

CREATE TABLE IF NOT EXISTS sc.captainAmerica  (
  shield_id String,
  ts UInt64,
  numbers Array(UInt32),
  date Date DEFAULT toDate(toDateTime(ts / 1000))
) ENGINE = MergeTree()
  PARTITION BY (shield_id, toYYYYMM(date))
  ORDER BY (shield_id);

CREATE TABLE IF NOT EXISTS sc.twoTestTable (
  item_id String,
  column_1 String,
  column_2 UInt32,
  column_3 String,
  column_4 String
) ENGINE = MergeTree()
  PARTITION BY (item_id)
  ORDER BY (item_id);

CREATE TABLE IF NOT EXISTS sc.threeTestTable (
  item_id String,
  column_4 String,
  column_5 String,
  column_6 String
) ENGINE = MergeTree()
  PARTITION BY (item_id)
  ORDER BY (item_id);
```

When done
```
DROP DATABASE sc;
```