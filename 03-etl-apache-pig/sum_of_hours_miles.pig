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
