CREATE KEYSPACE IF NOT EXISTS crime_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE crime_data;
CREATE TABLE IF NOT EXISTS crime_aggregate
(
    district               INT,
    month                  INT,
    primary_description    TEXT,
    count                  BIGINT,
    count_arrest           BIGINT,
    count_domestic         BIGINT,
    count_monitored_by_fbi BIGINT,
    PRIMARY KEY ((district), month, primary_description)
);