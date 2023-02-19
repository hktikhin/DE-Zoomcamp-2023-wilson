-- Q1
SELECT COUNT(1)
FROM production.fact_trips
WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
;
--61648442

-- Q2
-- 89.9/10.1


--Q3
SELECT COUNT(1)
FROM production.stg_fhv_tripdata  
;
--43244696

--Q4
SELECT COUNT(1)
FROM production.fact_fhv_trips 
;
--22998722

--Q5
-- main: jan

