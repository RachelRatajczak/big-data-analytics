# 04 — NoSQL database: HBase
Wide-column NoSQL operations on a Hadoop cluster — DDL, DML, bulk data import, and non-interactive shell commands

![HBase](https://img.shields.io/badge/Apache-HBase-purple) ![NoSQL](https://img.shields.io/badge/Database-NoSQL-blue) ![Hadoop](https://img.shields.io/badge/Storage-HDFS-green)

## Overview
This module covers HBase 2.1 — a wide-column NoSQL database built on HDFS. Work includes namespace and table creation (DDL), inserting/reading/updating/querying rows (DML), bulk-loading a 17,076-row trucking IoT dataset via MapReduce ImportTsv, and running HBase commands in non-interactive mode. HBase suits sparse, wide tables with high write throughput — common in IoT telemetry and clinical event logging.

## Table schema — rrata:truck_event
| Property | Value |
|----------|-------|
| Namespace | rrata |
| Table | rrata:truck_event |
| Row key | Composite key — format: driverId\|timestamp (HBASE_ROW_KEY at position 7 in CSV) |
| Column family | events |
| Columns (11) | driverId, truckId, eventTime, eventType, longitude, latitude, CorrelationId, driverName, routeId, routeName, eventDate |
| Row count | 17,076 |
| HBase version | 2.1.0-cdh6.3.2 |

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

## 4 — Bulk load: trucking IoT dataset
```bash
mkdir ~/CSC534BDA/hbase && cd ~/CSC534BDA/hbase
head truck_event_text_partition.csv
```
```bash
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.separator=, \
  -Dimporttsv.columns="events:driverId,events:truckId,events:eventTime,events:eventType,\
events:longitude,events:latitude,HBASE_ROW_KEY,events:CorrelationId,\
events:driverName,events:routeId,events:routeName,events:eventDate" \
  rrata:truck_event \
  hdfs://node00.sun:8020/user/data/CSC534BDA/Truck-IoT/truck_event_text_partition.csv
```
Result: 17,076 map input = map output records, 0 bad lines.
```bash
# Verify
hbase org.apache.hadoop.hbase.mapreduce.RowCounter 'rrata:truck_event'
# ROWS=17076
# Bytes Read=0, Bytes Written=0 — expected, RowCounter reads from HBase not HDFS
```
```ruby
# Scan first 2 rows
scan 'rrata:truck_event', {'LIMIT' => 2}
```
Output: composite row keys (e.g. `10|9223370572...`), all 11 columns under `events` family. Sample: driverName=George Vetticaden, routeName=Saint Louis to Tulsa, eventType=Normal.

---

## 5 — Assignment: insert, update, and query

### Insert new row (11 put commands)
```ruby
put 'rrata:truck_event', '20000', 'events:driverId',      'rrata'
put 'rrata:truck_event', '20000', 'events:truckId',       '999'
put 'rrata:truck_event', '20000', 'events:eventTime',     '01:01.1'
put 'rrata:truck_event', '20000', 'events:eventType',     'Normal'
put 'rrata:truck_event', '20000', 'events:longitude',     '-94.58'
put 'rrata:truck_event', '20000', 'events:latitude',      '37.03'
put 'rrata:truck_event', '20000', 'events:CorrelationId', '1000'
put 'rrata:truck_event', '20000', 'events:driverName',    'Rachel'
put 'rrata:truck_event', '20000', 'events:routeId',       '888'
put 'rrata:truck_event', '20000', 'events:routeName',     'UIS to Chicago'
put 'rrata:truck_event', '20000', 'events:eventDate',     '2025-09-29-11'
```

### Retrieve new row
```ruby
get 'rrata:truck_event', '20000'
```
Output: all 11 columns confirmed for row key `20000`.

### Update a column and confirm
```ruby
put 'rrata:truck_event', '20000', 'events:routeName', 'Chicago to UIS'
get 'rrata:truck_event', '20000', {COLUMN => 'events:routeName'}
```
Output: `events:routeName → Chicago to UIS`

### Retrieve two specific columns
```ruby
get 'rrata:truck_event', '20000', {COLUMN => ['events:driverName', 'events:routeName']}
```
Output: driverName=Rachel, routeName=Chicago to UIS.

### Non-interactive mode
```bash
echo "describe 'rrata:truck_event'" | hbase shell -n
```
Output: table ENABLED, column family `events`, VERSIONS=1, BLOOMFILTER=ROW, COMPRESSION=NONE, BLOCKCACHE=true, BLOCKSIZE=65536, TTL=FOREVER.

---

## Key concepts demonstrated
- Wide-column store — rows can have different columns, ideal for sparse IoT and clinical event data
- Row key design is critical — composite `driverId|timestamp` format enables efficient range scans by driver
- Column families are defined at table creation and group related columns
- `ImportTsv` uses MapReduce for bulk loading — far more efficient than row-by-row inserts
- `RowCounter` is a built-in MapReduce job for verifying bulk load completeness
- Non-interactive shell mode (`hbase shell -n`) enables HBase commands to be scripted in bash pipelines
- Each `put` creates a new timestamped cell version — visible as `timestamp=...` in get/scan output
