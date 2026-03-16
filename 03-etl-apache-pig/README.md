# 03 — ETL pipeline with Apache Pig
Pig Latin scripts to extract, transform, and analyze trucking IoT data — driver hours, miles, and speeding events

![Pig](https://img.shields.io/badge/Apache-Pig-blue) ![ETL](https://img.shields.io/badge/Pattern-ETL-orange) ![HDFS](https://img.shields.io/badge/Storage-HDFS-green)

## Overview
Two ETL pipelines built in Pig Latin against a Trucking IoT dataset in HDFS. Script 1 aggregates total hours and miles logged per driver by joining two CSVs. Script 2 filters and groups all overspeed events by driver. Both demonstrate core Pig Latin operations: loading schema-less data, filtering headers, transforming fields, grouping, aggregating, and joining relations.

## Dataset
| File | Contents |
|------|----------|
| `drivers.csv` | Driver records — driverId, name, and metadata |
| `timesheet.csv` | Per-driver logs — driverId, hours_logged, miles_logged |
| `truck_event_text_partition.csv` | Event-level telemetry — driverId, eventType, speed, location |

## Setup
```bash
mkdir ~/CSC534BDA/Pig && cd ~/CSC534BDA/Pig
hadoop fs -mkdir /user/csc/Pig
hadoop fs -put drivers.csv timesheet.csv truck_event_text_partition.csv /user/csc/Pig/
hadoop fs -ls /user/csc/Pig/
```

---

## Script 1 — driver hours and miles summary
```pig
-- Operations for drivers.csv
drivers = LOAD 'Pig/drivers.csv' USING PigStorage(',');
raw_drivers = FILTER drivers BY $0>1;
drivers_details = FOREACH raw_drivers GENERATE $0 AS driverId, $1 AS name;

-- Operations for timesheet.csv
timesheet = LOAD 'Pig/timesheet.csv' USING PigStorage(',');
raw_timesheet = FILTER timesheet BY $0>1;
timesheet_logged = FOREACH raw_timesheet GENERATE $0 AS driverId, $2 AS hours_logged,
$3 AS miles_logged;
grp_logged = GROUP timesheet_logged BY driverId;
sum_logged = FOREACH grp_logged GENERATE group AS driverId,
SUM(timesheet_logged.hours_logged) AS sum_hourslogged,
SUM(timesheet_logged.miles_logged) AS sum_mileslogged;
join_sum_logged = JOIN sum_logged BY driverId, drivers_details BY driverId;
join_data = FOREACH join_sum_logged GENERATE $0 AS driverId, $4 AS name, $1 AS hours_logged, $2 AS miles_logged;
dump join_data;
```
```bash
pig -f driver_hours_miles.pig
```

---

## Script 2 — overspeed event detection
```pig
drivers = LOAD 'Pig/truck_event_text_partition.csv' USING PigStorage(',') AS (
        driverId:int,
        truckId:int,
        eventTime:chararray,
        eventType:chararray,
        longitude: double,
        latitude: double,
        eventKey: chararray,
        CorrelationId: double,
        driverName: chararray,
        routeId: int,
        routeName: chararray,
        eventDate: chararray
);

overspeed_drivers = FILTER drivers BY eventType == 'Overspeed';
overspeed_group = GROUP overspeed_drivers BY driverId;
overspeed_events = FOREACH overspeed_group GENERATE group AS driverId, overspeed_drivers;
dump overspeed_events;
```
```bash
pig -f overspeed_drivers.pig
```

---

## Pig Latin operations used
| Operation | Purpose |
|-----------|---------|
| `LOAD / PigStorage` | Read CSV from HDFS with delimiter |
| `FILTER` | Remove headers and apply conditions |
| `FOREACH / GENERATE` | Select and transform fields |
| `GROUP` | Group rows by key into a bag |
| `SUM` | Aggregate values across grouped bag |
| `JOIN` | Combine two relations on shared key |
| `DUMP` | Print output to console |

## Key concepts demonstrated
- Pig Latin compiles down to MapReduce jobs automatically — no Java required
- Execution is lazy — deferred until DUMP or STORE triggers the pipeline
- Schema-on-read makes Pig flexible for raw CSV data without pre-defined tables
- Distributed JOIN across HDFS datasets without a relational database
- GROUP → FOREACH → SUM aggregation pattern mirrors clinical claims and EHR data rollups
