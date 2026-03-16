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
