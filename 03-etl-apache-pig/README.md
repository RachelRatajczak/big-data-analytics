# 03 — ETL pipeline with Apache Pig
Pig Latin scripts to extract, transform, and analyze trucking IoT data — driver hours, miles, and speeding events

> Graduate coursework — University of Illinois Springfield, Data Analytics (CSC 534)

![Pig](https://img.shields.io/badge/Apache-Pig-blue) ![ETL](https://img.shields.io/badge/Pattern-ETL-orange) ![HDFS](https://img.shields.io/badge/Storage-HDFS-green)

## Overview
Two ETL pipelines built in Pig Latin against a Trucking IoT dataset in HDFS. Script 1 (`sum_of_hours_miles.pig`) aggregates total hours and miles logged per driver by joining two CSVs on driverId. Script 2 (`overspeed_drivers.pig`) filters truck telemetry for speeding violations and groups them by driver. Both use positional field notation and demonstrate core Pig Latin operations: loading, filtering headers, transforming, grouping, aggregating, and joining.

## Dataset
| File | Fields used |
|------|-------------|
| `drivers.csv` | `$0` driverId, `$1` name — header filtered with `$0>1` |
| `timesheet.csv` | `$0` driverId, `$2` hours_logged, `$3` miles_logged |
| `truck_event_text_partition.csv` | Full named schema: driverId, truckId, eventTime, eventType, longitude, latitude, eventKey, CorrelationId, driverName, routeId, routeName, eventDate |

## Setup
```bash
mkdir ~/CSC534BDA/Pig && cd ~/CSC534BDA/Pig
hadoop fs -mkdir /user/csc/Pig
hadoop fs -put drivers.csv timesheet.csv truck_event_text_partition.csv /user/csc/Pig/
hadoop fs -ls /user/csc/Pig/
```

---

## Script 1 — sum_of_hours_miles.pig
Joins `drivers.csv` and `timesheet.csv` on driverId to produce total hours and miles per driver. Uses positional notation since no schema is defined on load. Header rows excluded by filtering `$0>1`.
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
join_data = FOREACH join_sum_logged GENERATE $0 AS driverId, $4 AS name, $1 AS hours_logged,
$2 AS miles_logged;
dump join_data;
```
```bash
pig -f sum_of_hours_miles.pig
```

---

## Script 2 — overspeed_drivers.pig
Loads truck event telemetry with a full named schema, filters for `'Overspeed'` events, then groups violations by driverId.
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
| `FILTER` | Remove headers (`$0>1`) and filter by value |
| `FOREACH / GENERATE` | Select and rename fields — positional or named |
| `GROUP` | Group rows by key into a bag |
| `SUM` | Aggregate values across grouped bag |
| `JOIN` | Combine two relations on a shared key |
| `dump` | Print output to console |

## Key concepts demonstrated
- Positional notation (`$0`, `$1`...) loads schema-less CSVs without pre-defining structure
- Filtering `$0>1` is a practical pattern for stripping CSV headers in Pig
- Pig execution is lazy — pipeline only runs when `dump` or `STORE` is triggered
- Distributed JOIN across HDFS datasets without a relational database
- GROUP → FOREACH → SUM mirrors rollup patterns used in clinical claims and EHR pipelines
