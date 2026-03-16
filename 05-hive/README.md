# 05 — Data analytics with Hive
HiveQL for data warehousing, OLAP aggregations, driver risk analysis, and Titanic survival statistics

![Hive](https://img.shields.io/badge/Apache-Hive-yellow) ![OLAP](https://img.shields.io/badge/Pattern-OLAP-blue) ![HiveQL](https://img.shields.io/badge/Language-HiveQL-green)

## Overview
Two Hive exercises covering SQL-on-Hadoop analytics. Exercise 1 covers table creation, data loading, and analytical queries on Trucking IoT data. Exercise 2 covers advanced OLAP — GROUPING SETS, ROLLUP, CUBE — and a Titanic survival analysis using OpenCSVSerde for quoted CSV parsing.

---

## Exercise 1 — Data analytics with DW/OLAP

### Create tables
```sql
CREATE TABLE csc534.rrata_drivers (driverId INT,
    name STRING, ssn BIGINT, location STRING,
    certified STRING, wageplan STRING)
COMMENT 'drivers'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES('skip.header.line.count'='1');

CREATE TABLE csc534.rrata_timesheet (
    driverId INT, week INT,
    `hours-logged` INT, `miles-logged` INT)
COMMENT 'timesheet'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE TABLE csc534.rrata_truck_event (
    driverId INT, truckId INT, eventTime STRING,
    eventType STRING, longitude DOUBLE, latitude DOUBLE,
    eventKey STRING, CorrelationId DOUBLE, driverName STRING,
    routeId BIGINT, routeName STRING, eventDate STRING)
COMMENT 'truck_event'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');
```

### Verify tables
```sql
SHOW TABLES;
DESCRIBE csc534.rrata_drivers;
DESCRIBE csc534.rrata_timesheet;
DESCRIBE csc534.rrata_truck_event;
```

### Load data
```sql
LOAD DATA LOCAL INPATH '/home/data/CSC534BDA/datasets/Truck-IoT/drivers.csv'
  INTO TABLE csc534.rrata_drivers;
LOAD DATA LOCAL INPATH '/home/data/CSC534BDA/datasets/Truck-IoT/timesheet.csv'
  INTO TABLE csc534.rrata_timesheet;
LOAD DATA LOCAL INPATH '/home/data/CSC534BDA/datasets/Truck-IoT/truck_event_text_partition.csv'
  INTO TABLE csc534.rrata_truck_event;

SELECT * FROM csc534.rrata_drivers LIMIT 5;
SELECT count(*) FROM csc534.rrata_drivers;
SELECT count(*) FROM csc534.rrata_timesheet;
SELECT count(*) FROM csc534.rrata_truck_event;
```

### Aggregate hours and miles per driver
```sql
SELECT driverId, sum(`hours-logged`), sum(`miles-logged`)
FROM csc534.rrata_timesheet
GROUP BY driverId;
```

### Join driver names with aggregated totals
```sql
SELECT d.driverId, d.name, t.total_hours, t.total_miles
FROM csc534.rrata_drivers d
JOIN (SELECT driverId, sum(`hours-logged`) total_hours, sum(`miles-logged`) total_miles
      FROM csc534.rrata_timesheet
      GROUP BY driverId) t
ON (d.driverId = t.driverId);
```

---

## Exercise 2 — Advanced data analytics

### Dimensional aggregation
```sql
-- Events by driver and event type
SELECT driverId, eventType, count(*)
FROM csc534.rrata_truck_event
GROUP BY driverId, eventType;

-- Total events per driver
SELECT driverId, count(*) AS total_events
FROM csc534.rrata_truck_event
GROUP BY driverId;

-- Combined with UNION ALL
SELECT driverId, eventType, count(*) AS total
FROM csc534.rrata_truck_event
GROUP BY driverId, eventType
UNION ALL
SELECT driverId, null as eventType, count(*) AS total
FROM csc534.rrata_truck_event
GROUP BY driverId;

-- GROUPING SETS — single-stage equivalent (ran in 6 seconds)
SELECT driverId, eventType, count(*) AS occurrence
FROM csc534.rrata_truck_event
GROUP BY driverId, eventType
GROUPING SETS ((driverId, eventType), driverId);
```

### Driver risk factor analysis
```sql
-- Unusual events table (ORC)
CREATE TABLE csc534.rrata_unusual_events STORED AS ORC AS
SELECT driverId, count(*) AS occurrence
FROM csc534.rrata_truck_event
WHERE eventType != 'Normal'
GROUP BY driverId;

-- Totals table (ORC)
CREATE TABLE csc534.rrata_totals STORED AS ORC AS
SELECT driverId, sum(`hours-logged`) AS total_hours, sum(`miles-logged`) AS total_miles
FROM csc534.rrata_timesheet
GROUP BY driverId;

-- Join tables
CREATE TABLE csc534.rrata_joined STORED AS ORC AS
SELECT u.driverId, u.occurrence, t.total_hours, t.total_miles
FROM csc534.rrata_unusual_events u
JOIN csc534.rrata_totals t ON (u.driverId = t.driverId);

-- Calculate risk factor (lower = higher risk)
SELECT driverId, occurrence, total_hours, total_miles,
       total_miles/occurrence AS riskfactor
FROM csc534.rrata_joined
ORDER BY riskfactor ASC
LIMIT 5;
```

### ROLLUP
```sql
SET hive.cli.print.header=true;
SELECT driverId, eventType, COUNT(*) AS occurrence
FROM csc534.rrata_truck_event
GROUP BY ROLLUP(driverId, eventType)
ORDER BY driverId, eventType
LIMIT 10;
```
Produces: grand total → subtotal per driverId → count per driverId+eventType.

### CUBE
```sql
SELECT driverId, eventType, count(*) AS occurrence
FROM csc534.rrata_truck_event
GROUP BY CUBE(driverId, eventType)
ORDER BY driverId, eventType
LIMIT 10;
```
All combinations: grand total, per-eventType, per-driverId, per driverId+eventType. Total = 17,075 events.

### Titanic survival analysis
```sql
-- Create table with OpenCSVSerde (handles quoted Name field with embedded commas)
use csc534;
CREATE TABLE rrata_titanic (
    PassengerId INT, Survived INT, Pclass INT, Name STRING,
    Sex STRING, Age DOUBLE, SibSp INT, Parch INT,
    Ticket STRING, Fare DOUBLE, Cabin STRING, Embarked STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
STORED AS TEXTFILE;
```
```bash
# Remove header
hdfs dfs -cat /user/rrata/titanic/titanic.csv | tail -n +2 | \
  hdfs dfs -put - /user/rrata/titanic/titanic_noheader.csv
```
```sql
LOAD DATA INPATH '/user/rrata/titanic/titanic_noheader.csv' INTO TABLE rrata_titanic;
SELECT count(*) FROM rrata_titanic;  -- 891 rows confirmed
```
```sql
-- Survival rate overall
set hive.cli.print.header=true;
SELECT Survived, count(*) AS total,
       count(*) / total_count.total AS survival_death_rate
FROM rrata_titanic,
     (SELECT count(*) AS total FROM rrata_titanic) total_count
GROUP BY Survived, total_count.total;
-- 342 survived (38.4%) · 549 died (61.6%)

-- Survival rate by sex
SELECT Survived, Sex, count(*) AS total,
       count(*) / total_count.total AS survival_death_rate
FROM rrata_titanic,
     (SELECT count(*) AS total FROM rrata_titanic) total_count
GROUP BY Survived, Sex, total_count.total
ORDER BY Survived, Sex;
-- 233 females survived (26.2%) · 109 males survived (12.2%)
-- 81 females died (9.1%) · 468 males died (52.5%)
```

---

## Key concepts demonstrated
- Hive compiles HiveQL to MapReduce/Tez jobs — SQL interface over distributed data
- GROUPING SETS runs multiple aggregations in one stage vs. UNION ALL requiring multiple stages
- ROLLUP produces hierarchical subtotals; CUBE produces all possible aggregation combinations
- ORC format improves analytical query performance over TEXTFILE
- OpenCSVSerde handles quoted fields with embedded delimiters — essential for real-world messy data
- Risk factor calculation (miles/occurrences) maps directly to clinical safety metrics and outcome analysis
