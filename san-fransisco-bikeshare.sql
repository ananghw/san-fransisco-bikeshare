  #INTERMEDIATE_QUESTION1
SELECT
  DATE(DATE_TRUNC(start_date,MONTH)) AS month_year,
  AVG(duration_sec/60) AS average_in_minute
FROM
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE
  start_date BETWEEN '2014-01-01'
  AND '2017-12-31'
GROUP BY
  1
ORDER BY
  1;
  #INTERMEDIATE_QUESTION2
SELECT
  regions.name AS region_name,
  COUNT(DISTINCT(trip_id)) AS total_trips,
  COUNT(DISTINCT(bike_number)) AS total_bike
FROM
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` trips
INNER JOIN
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` info
ON
  trips.start_station_id = info.station_id
INNER JOIN
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` regions
ON
  info.region_id=regions.region_id
WHERE
  trips.start_date BETWEEN '2014-01-01'
  AND '2017-12-31'
GROUP BY
  1
ORDER BY
  1;
  #INTERMEDIATE_QUESTION3
SELECT
  DISTINCT(member_gender) AS gender,
  MIN(2022-member_birth_year) OVER (PARTITION BY member_gender) AS youngest_age,
  MAX(2022-member_birth_year) OVER (PARTITION BY member_gender) AS oldest_age
FROM
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE
  start_date BETWEEN '2014-01-01'
  AND '2017-12-31'
  AND member_gender IS NOT NULL
ORDER BY
  1;
  #INTERMEDIATE_QUESTION4
WITH
  TEMPORARY AS (
  SELECT
    C.name AS region_name,
    trip_id,
    duration_sec,
    start_date,
    start_station_name,
    member_gender
  FROM
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` A
  INNER JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` B
  ON
    A.start_station_id = B.station_id
  INNER JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` C
  ON
    B.region_id=C.region_id
  WHERE
    start_date BETWEEN '2014-01-01'
    AND '2017-12-31'
    AND member_gender IS NOT NULL )
SELECT
  region_name,
  trip_id,
  duration_sec,
  start_date,
  start_station_name,
  member_gender
FROM (
  SELECT
    *,
    MAX(start_date) OVER (PARTITION BY (region_name)) AS latest_trip
  FROM
    TEMPORARY )
WHERE
  start_date = latest_trip
ORDER BY
  1;
  #INTERMEDIATE_QUESTION5
WITH
  total AS(
  SELECT
    DATE(DATE_TRUNC(start_date,DAY)) AS start_date,
    region_table.name AS region_name,
    COUNT(DISTINCT(trip_id)) AS total_trips
  FROM
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` AS trip_table
  INNER JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS info_table
  ON
    trip_table.start_station_id=info_table.station_id
  INNER JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions`AS region_table
  ON
    info_table.region_id=region_table.region_id
  WHERE
    start_date BETWEEN '2017-11-01'
    AND '2017-12-31'
  GROUP BY
    1,
    2)
SELECT
  *
FROM
  total;
  #ADVANCED_QUESTION1
WITH
  highest_region_trip AS(
  SELECT
    region_table.name AS region_name,
    COUNT(DISTINCT(trip_id)) AS number_of_trips,
    ROW_NUMBER()OVER(ORDER BY (COUNT(DISTINCT(trip_id)))DESC) AS rank
  FROM
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` AS trip_table
  INNER JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS info_table
  ON
    trip_table.start_station_id=info_table.station_id
  INNER JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions`AS region_table
  ON
    info_table.region_id=region_table.region_id
  GROUP BY
    1),
  helper_table AS(
  SELECT
    region_table.name AS region_name,
    COUNT(DISTINCT(trip_id)) AS number_of_trips,
    EXTRACT(MONTH
    FROM
      start_date)AS month,
    EXTRACT(YEAR
    FROM
      start_date)AS year,
  FROM
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` AS trip_table
  INNER JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS info_table
  ON
    trip_table.start_station_id=info_table.station_id
  INNER JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions`AS region_table
  ON
    info_table.region_id=region_table.region_id
  WHERE
    start_date BETWEEN '2014-01-01'
    AND '2017-12-31'
    AND region_table.name IN (
    SELECT
      region_name
    FROM
      highest_region_trip
    WHERE
      rank = 1)
  GROUP BY
    1,
    3,
    4
  ORDER BY
    1 )
SELECT
  region_name AS region,
  year AS year,
  month AS month,
  number_of_trips,
  growth_percentages
FROM (
  SELECT
    *,
    CONCAT(ROUND(((number_of_trips) - LEAD(number_of_trips)OVER(ORDER BY month DESC))/LEAD(number_of_trips)OVER(ORDER BY month DESC)*100,2),'%') AS growth_percentages
  FROM
    helper_table)
ORDER BY
  2 DESC,
  3 DESC;
  #ADVANCED_QUESTION2
WITH
  cohort_items AS(
  SELECT
    author AS author,
    MIN(DATE(DATE_TRUNC(time_ts,MONTH))) AS cohort_month,
  FROM
    `bigquery-public-data.hacker_news.stories`
  GROUP BY
    1),
  user_activities AS (
  SELECT
    act.author AS author,
    DATE_DIFF(DATE(DATE_TRUNC(time_ts,MONTH)), cohort.cohort_month, MONTH ) AS month_number,
  FROM
    `bigquery-public-data.hacker_news.stories` act
  LEFT JOIN
    cohort_items AS cohort
  ON
    act.author = cohort.author
  WHERE
    EXTRACT(year
    FROM
      cohort.cohort_month) IN (2014)
  GROUP BY
    1,
    2),
  cohort_size AS (
  SELECT
    cohort_month,
    COUNT(1) AS num_users
  FROM
    cohort_items
  GROUP BY
    1
  ORDER BY
    1),
  retention_table AS (
  SELECT
    C.cohort_month,
    A.month_number AS month_number,
    COUNT(1) AS num_users
  FROM
    user_activities A
  LEFT JOIN
    cohort_items C
  ON
    A.author = C.author
  GROUP BY
    1,
    2)
SELECT
  B.cohort_month,
  S.num_users AS cohort_size,
  B.month_number,
  B.num_users AS total_users,
  CAST(B.num_users AS decimal)/ S.num_users*100 AS percentage
FROM
  retention_table B
LEFT JOIN
  cohort_size S
ON
  B.cohort_month = S.cohort_month
WHERE
  B.cohort_month IS NOT NULL
ORDER BY
  1,
  3;