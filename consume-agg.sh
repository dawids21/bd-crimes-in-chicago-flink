#!/bin/bash
docker exec -it cassandra cqlsh -e "SELECT * FROM crime_data.crime_aggregate;"