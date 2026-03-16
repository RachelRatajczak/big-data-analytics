# 04 — NoSQL database: HBase
Wide-column NoSQL operations on a Hadoop cluster — DDL, DML, bulk data import, and non-interactive shell commands

![HBase](https://img.shields.io/badge/Apache-HBase-purple) ![NoSQL](https://img.shields.io/badge/Database-NoSQL-blue) ![Hadoop](https://img.shields.io/badge/Storage-HDFS-green)

## Overview
This module covers HBase — a wide-column NoSQL database built on HDFS. Work includes namespace and table creation (DDL), inserting/reading/updating/querying rows (DML), bulk-loading a 17,076-row trucking IoT dataset via a MapReduce import job, and running HBase commands in non-interactive mode. HBase suits sparse, wide tables with high write throughput — common in IoT telemetry and clinical event logging.

## Table schema — rrata:truck_event
| Property | Value |
|----------|-------|
| Namespace | rrata |
| Table | rrata:truck_event |
| Row key | eventKey (position 7 in CSV) |
| Column family | events |
| Columns | driverId, truckId, eventTime, eventType, longitude, latitude, CorrelationId, driverName, routeId, routeName, eventDate |
| Row count | 17,076 |

---

## 1 — Start HBase shell
```bash
hbase shell
status
```

## 2 — DDL: namespace and table creation
```ruby
create_namespace 'rrata'
list_namespace
create 'rrata:mytable_tbl', 'mycolfam_cf'
create 'rrata:truck_event', 'events'
```

## 3 — DML: insert, read, scan
```ruby
put 'rrata:mytable_tbl', 'myrowkey_key', 'mycolfam_cf:mycolname_col', 'Welcome to My HBase!'
get 'rrata:mytable_tbl', 'myrowkey_key'
scan 'rrata:mytable_tbl'
```
`get` retrieves a single row by row key. `scan` returns all rows.

## 4 — Bulk load: trucking IoT dataset
```bash
# Inspect data
mkdir ~/CSC534BDA/hbase && cd ~/CSC534BDA/hbase
head truck_event_text_partition.csv

# Import via MapReduce
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.separator=',' \
  -Dimporttsv.columns="HBASE_ROW_KEY,events:driverId,events:truckId,events:eventTime,\
events:eventType,events:longitude,events:latitude,events:eventKey,\
events:CorrelationId,events:driverName,events:routeId,events:routeName,events:eventDate" \
  rrata:truck_event \
  /user/csc/Pig/truck_event_text_partition.csv
```
Result: 17,076 map input = map output records, 0 bad lines.
```bash
# Verify row count
hbase org.apache.hadoop.hbase.mapreduce.RowCounter 'rrata:truck_event'
# Output: ROWS = 17076
```

---

## 5 — Assignment: insert, update, and query

### Insert new row (11 put commands)
```ruby
put 'rrata:truck_event', '20000', 'events:driverId',      '20000'
put 'rrata:truck_event', '20000', 'events:truckId',       '201'
put 'rrata:truck_event', '20000', 'events:eventTime',     '2024-01-01 08:00:00.000'
put 'rrata:truck_event', '20000', 'events:eventType',     'Normal'
put 'rrata:truck_event', '20000', 'events:longitude',     '-90.1994'
put 'rrata:truck_event', '20000', 'events:latitude',      '38.6270'
put 'rrata:truck_event', '20000', 'events:eventKey',      '20000'
put 'rrata:truck_event', '20000', 'events:CorrelationId', '20000'
put 'rrata:truck_event', '20000', 'events:driverName',    'Rachel Ratajczak'
put 'rrata:truck_event', '20000', 'events:routeId',       '160405074'
put 'rrata:truck_event', '20000', 'events:routeName',     'Saint Louis to Memphis'
```

### Retrieve new row
```ruby
get 'rrata:truck_event', '20000'
```

### Update a column and confirm
```ruby
put 'rrata:truck_event', '20000', 'events:routeName', 'Chicago to UIS'
get 'rrata:truck_event', '20000', {COLUMNS => ['events:routeName']}
```

### Retrieve two specific columns
```ruby
get 'rrata:truck_event', '20000', {COLUMNS => ['events:driverName', 'events:routeName']}
```

### Non-interactive mode
```bash
echo "describe 'rrata:truck_event'" | hbase shell
```
Runs HBase from the Linux shell without entering the interactive prompt — useful for scripting and automation.

---

## Key concepts demonstrated
- Wide-column store — rows can have different columns, ideal for sparse IoT and clinical event data
- Row key design is critical — `eventKey` chosen because it uniquely identifies each truck event
- Column families group related columns and are defined at table creation
- `ImportTsv` uses MapReduce to bulk-load large datasets without row-by-row inserts
- `RowCounter` is a built-in MapReduce job for verifying bulk load completeness
- Non-interactive shell mode enables HBase commands to be scripted in bash pipelines
