
-- data: https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
-- https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv 
-- total record count 
SELECT count(1)
FROM public.yellow_taxi_trips_2019
;

-- q1: How many taxi trips were totally made on January 15?
SELECT COUNT(*)
FROM public.yellow_taxi_trips_2019
WHERE 
	lpep_pickup_datetime::DATE = '2019-01-15'
;
-- out: 20689

--q2: Which was the day with the largest trip distance?
SELECT lpep_pickup_datetime::DATE, MAX(trip_distance) 
FROM public.yellow_taxi_trips_2019
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1
;

-- q3: In 2019-01-01 how many trips had 2 and 3 passengers?
SELECT passenger_count, COUNT(*)
FROM public.yellow_taxi_trips_2019
WHERE 
    lpep_pickup_datetime::DATE = '2019-01-01'
GROUP BY 1
HAVING passenger_count IN (2, 3)
;

--Q4
-- For the passengers picked up in the Astoria Zone which was the drop up zone that 
-- had the largest tip?


-- use "" to suppress error from postres if column name
-- start with capital letter 
-- Astoria
SELECT ptz."Zone", 
    dtz."Zone", 
    MAX(yt."tip_amount")
FROM public.yellow_taxi_trips_2019 as yt
JOIN public.taxi_zone as ptz
ON ptz."LocationID" = yt."PULocationID"
JOIN public.taxi_zone as dtz
ON dtz."LocationID" = yt."DOLocationID"
GROUP BY 1, 2
HAVING ptz."Zone" = 'Astoria'
ORDER BY 3 DESC 
LIMIT 1
;




