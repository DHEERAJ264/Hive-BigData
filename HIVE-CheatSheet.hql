-------------------------------------------------------
-- prepare data
-------------------------------------------------------
-- 1. Log into selected cluster
-- 2. Load data into your sandbox (and check if the upload was successful)
-- >> hdfs dfs -put <file_name> /user/<q_user>
-- >> hdfs dfs -mkdir -p 
-- >> hdfs dfs -put 
-- >> hdfs dfs -ls 

-------------------------------------------------------
-- Create a database and external table
-------------------------------------------------------
-- >> beeline -r 
CREATE DATABASE <db_name>
LOCATION '<location_path>';
--No rows affected 

-- Create an external table 
CREATE EXTERNAL TABLE <db_name>.<exttable_name> (
  col1 datatype1,
  col2 datatype2,
  ....
  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '<location_path>'
TBLPROPERTIES ('skip.header.line.count'='1');


-------------------------------------------------------
-- (optional) test the load: How to be sure that all important data is loaded?
-------------------------------------------------------
SELECT COUNT(*)
FROM <db_name>.<exttable_name>
WHERE <col_name> IS NOT NULL
   OR <col_name> != ''
   OR length(<col_name>) > 1
....;

SELECT COUNT(*)
FROM <db_name>.<exttable_name>;


--*********************************************
-- Managed table 
--*********************************************

-------------------------------------------------------
-- Create a managed table
-------------------------------------------------------
CREATE TABLE <db_name>.<inttable_name> (
  col1 datatype1,
  col2 datatype2,
  ....
  )
PARTITIONED BY (<col_name> <datatype>)
STORED AS ORC
LOCATION '<location_path>';
--No rows affected 

-------------------------------------------------------
-- Static partitioning
-------------------------------------------------------
-- 1. See all the timestamps and pick one:
SELECT DISTINCT <column_name>
FROM <db_name>.<exttable_name>;


-- 2. Load data into a managed table using static partitioning
INSERT INTO TABLE <db_name>.<inttable_name>
PARTITION (day='2018-01-25')
SELECT * 
FROM <db_name>.<exttable_name>
WHERE time_stamp LIKE '2018-01-25%';
--No rows affected 

-- 3. Compare count for the same day in both tables



-------------------------------------------------------
-- Dynamic partitioning
-------------------------------------------------------
-- 1. Load data into the table using dynamic partitioning
INSERT INTO TABLE <db_name>.<inttable_name>
PARTITION (<partition_column_name>)
SELECT *, to_date(time_stamp) as day
FROM <db_name>.<exttable_name>
WHERE NOT (`time_stamp` LIKE '2018-01-25%');

-- 2. Compare count of all the data in both tables


-------------------------------------------------------
-- (optional) Partitioning reflection on HDFS
-------------------------------------------------------
-- 1. Find location of your table
DESCRIBE FORMATTED <db_name>.<inttable_name>;

-- 2. Check how your Managed table data looks on HDFS
-- >> hdfs dfs -ls 

-- 3. Drop one partition from the managed table
ALTER TABLE <db_name>.<inttable_name>
DROP PARTITION (day='2018-01-25');

-- 4. Have a look on HDFS once again
-- >> hdfs dfs -ls 

-- 5. Load the dropped partition into managed table back again
INSERT INTO TABLE <db_name>.<inttable_name>
PARTITION (day='2018-01-25')
SELECT * 
FROM <db_name>.<exttable_name>
WHERE time_stamp LIKE '2018-01-25%';

-- No rows affected

-------------------------------------------------------
-- (optional) More advanced Testing
-------------------------------------------------------
-- Run a join to check if all data was loaded correctly
WITH
v1 as (
    SELECT CONCAT(robot, ' ', time_stamp) as id, COUNT(*) as csv_c
    FROM <db_name>.<exttable_name>
    GROUP BY robot, time_stamp), 
v2 as (
    SELECT CONCAT(robot, ' ', time_stamp) as id, COUNT(*) as orc_c
    FROM <db_name>.<inttable_name>
    GROUP BY robot, time_stamp)
SELECT 
    v1.id, v1.csv_c, v2.orc_c, 
    (v1.csv_c - v2.orc_c) as difference 
FROM v1 
LEFT JOIN v2 
    on v1.id = v2.id 
ORDER BY v1.id;


--****************************************************
-- Aggregation
--****************************************************

-------------------------------------------------------
-- Find the average of output_power
-------------------------------------------------------
SELECT
    robot, time_stamp,
    AVG(output_power) as avg_power
FROM <db_name>.<inttable_name>
GROUP BY time_stamp, robot;


--****************************************************
-- Windowing
--****************************************************
SELECT
    robot, time_stamp, measurement_point_number,
    (MAX(target_speed) OVER w > target_speed + 15) as acceleration_phase
FROM <db_name>.<inttable_name>
ORDER BY RAND()
WINDOW w AS (PARTITION BY robot, time_stamp
             ORDER BY measurement_point_number 
             RANGE BETWEEN 0 FOLLOWING AND 15 FOLLOWING)
LIMIT 20;

     
     
--****************************************************
-- One possible solution
--****************************************************    
set hivevar:window_size = 15;

WITH t_acceleration AS (
    SELECT 
        CONCAT(robot, ' ', time_stamp) as id,
        measurement_point_number,
        output_power,
        (MAX(target_speed) OVER w > target_speed + ${window_size}) = true as acceleration
    FROM <db_name>.<inttable_name>
    WINDOW w AS (PARTITION BY robot, time_stamp
                 ORDER BY measurement_point_number 
                 RANGE BETWEEN 0 FOLLOWING AND ${window_size} FOLLOWING)
)
SELECT
    id,
    MIN(measurement_point_number) as start_pos,
    MAX(measurement_point_number) as end_pos,
    AVG(output_power) as avg_power
FROM t_acceleration
GROUP BY id, acceleration
HAVING acceleration = true;


--****************************************************
-- CLEANUP
--****************************************************   
DROP DATABASE <db_name> CASCADE;
-- No rows affected 
-- >> >> hdfs dfs -rm -R /user/<quser>/prodlog 
--  INFO fs.TrashPolicyDefault: Moved: <> to trash at: <>
