
-- DOCO: Creates external table on AWS open data set for OpenAQ
	-- OpenQA is an initiative to consolidate 
drop table if exists openaq;
create external table `openaq`(
`date` struct<utc:string,local:string> COMMENT 'from deserializer',
`parameter` string COMMENT 'from deserializer',
`location` string COMMENT 'from deserializer',
`value` float COMMENT 'from deserializer',
`unit` string COMMENT 'from deserializer',
`city` string COMMENT 'from deserializer',
`attribution` array<struct<name:string, url:string>> COMMENT 'from deserializer',
`averagingperiod` struct<unit:string,value:float> COMMENT 'from deserializer',
`coordinates` struct<latitude:string,longitude:string> COMMENT 'from deserializer',
`country` string COMMENT 'from deserializer',
`sourcename` string COMMENT 'from deserializer',
`sourcetype` string COMMENT 'from deserializer',
`mobile` string COMMENT 'from deserializer') 
row FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
STORED as INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
LOCATION 's3://openaq-fetches/realtime-gzipped' 
TBLPROPERTIES ( 'transient_lastDdlTime' = '1518373755')


-- DOCO: Create a quick parquet version of the openaq table to work with
drop table if exists openaq_pqt;
create table openaq_pqt
WITH (
      format = 'Parquet',
      external_location = 's3://sickdat-query-engine-talk/openaq_pqt/',
      parquet_compression = 'SNAPPY',
      partitioned_by = ARRAY['date_year','date_month'] 
	)
as
select 
  openaq.*,
 sha256(to_utf8(concat(location, city, country))) as hash_key,
 cast(openaq.coordinates.latitude as decimal(12,8)) as latitude_numeric,
 cast(openaq.coordinates.longitude as decimal(12,8)) as longitude_numeric,
 cast(year(from_iso8601_timestamp(openaq.date.utc)) as int) as date_year,
 cast(month(from_iso8601_timestamp(openaq.date.utc)) as int) as date_month
 from openaq
 where cast(year(from_iso8601_timestamp(openaq.date.utc)) as int)>2010;

-- insert the rest because we have too many partitions for Athena
insert INTO openaq_pqt
select
  openaq.*,
  sha256(to_utf8(concat(location, city, country))) as hash_key,
 cast(year(from_iso8601_timestamp(openaq.date.utc)) as int) as date_year,
 cast(month(from_iso8601_timestamp(openaq.date.utc)) as int) as date_month
FROM openaq
where cast(year(from_iso8601_timestamp(openaq.date.utc)) as int) <=2010;

-- DOCO: Create dimension of known pollutants.
  -- Enriched with a custom description
  -- TODO: Automate creation of table with ids (will need a small table for the descriptions)
DROP TABLE IF EXISTS pollutant_dim_pqt;
CREATE external TABLE pollutant_dim_pqt (
	pollutant_id bigint,
	pollutant_parameter STRING,
	descriptiony STRING
)
STORED as PARQUET
LOCATION 's3://sickdat-query-engine-talk/pollutant_dim_pqt/' 
tblproperties ("parquet.compression"="SNAPPY");

insert into pollutant_dim_pqt
SELECT * FROM (
    VALUES
(1, 'no2', 'Nitrogen Dioxide'),
(2, 'o3', 'Ozone'),
(3, 'pm10', 'Particulate Matter - Small <10 µm'),
(4, 'pm25', 'Particulate Matter - Very Small <2.5 µm'),
(5, 'so2', 'Sulphur dioxide'),
(6, 'bc', 'Black Carbon'),
(7, 'co', 'Carbon monoxide')
) AS t (pollutant_id, _parameter, description);

-- select * from "default".pollutant_dim ;

-- DOCO: Create date dimension
  -- Created in Redshift, need to improve my Presto-foo to port
  -- Used: https://elliotchance.medium.com/building-a-date-dimension-table-in-redshift-6474a7130658 
DROP TABLE if exists date_dim;
CREATE external TABLE date_dim (
  date_id INT,
  full_date STRING,
  au_format_date CHAR(10),
  us_format_date CHAR(10),
  year_number SMALLINT,
  year_week_number SMALLINT,
  year_day_number SMALLINT,
  au_fiscal_year_number SMALLINT,
  us_fiscal_year_number SMALLINT,
  qtr_number SMALLINT,
  au_fiscal_qtr_number SMALLINT,
  us_fiscal_qtr_number SMALLINT,
  month_number SMALLINT,
  month_name CHAR(9) ,
  month_day_number SMALLINT,
  week_day_number SMALLINT,
  day_name CHAR(9) ,
  day_is_weekday SMALLINT,
  day_is_last_of_month SMALLINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
's3://sickdat-query-engine-talk/date_dim/'
TBLPROPERTIES (
'compressionType'='none',
'delimiter'=',',
'objectCount'='1',
'skip.header.line.count'='1',
'typeOfData'='file')


DROP TABLE if exists location_dim_pqt;
CREATE external TABLE location_dim_pqt (
	location_id bigint,
	location STRING,
	city STRING,
	country STRING,
	hash_key BINARY
)
STORED as PARQUET
LOCATION 's3://sickdat-query-engine-talk/location_dim_pqt/' 
tblproperties ("parquet.compression"="SNAPPY");

insert into location_dim_pqt(location_id, location, city, country, hash_key)	
with locations as ( 
	select distinct location, city, country from openaq
) 
select 
	row_number() over (order by location desc) as location_id, location, city, country, sha256(to_utf8(concat(location, city, country))) as hash_key from locations

-- DOCO: creating averaging_period_dim, can automate
select count(*) from "default".location_dim_pqt;
 	
DROP TABLE if exists averaging_period_dim_pqt;
CREATE external TABLE averaging_period_dim_pqt (
	averaging_period_id bigint,
	unit STRING,
	period_value FLOAT
)
STORED as PARQUET
LOCATION 's3://sickdat-query-engine-talk/averaging_period_dim/' 
tblproperties ("parquet.compression"="SNAPPY");

insert into averaging_period_dim_pqt(averaging_period_id, unit, period_value)	
with averaging_periods as ( 
	select distinct averagingperiod.unit, averagingperiod.value as period_value from openaq_pqt
) 
select 
	row_number() over (order by unit desc) as averaging_period_id, unit, period_value from averaging_periods
	

--DOCO: Create source_dim, can automate
DROP TABLE if exists source_dim_pqt;
CREATE external TABLE source_dim_pqt (
  source_id BIGINT,
  sourcename STRING,
  sourcetype STRING
)
STORED as PARQUET
LOCATION 's3://sickdat-query-engine-talk/source_dim_pqt/' 
tblproperties ("parquet.compression"="SNAPPY");

insert into source_dim_pqt(source_id, sourcename, sourcetype)	
with sources as ( 
	select distinct sourcename, sourcetype from openaq_pqt
) 
select 
	row_number() over (order by sourcename desc) as source_id, sourcename, sourcetype from sources
	
select * from "default".source_dim_pqt;


select distinct latitude_numeric, longitude_numeric from 
 (select * from "default".openaq_pqt order by date.utc);

DROP TABLE if exists coordinate_dim_pqt;
CREATE external TABLE coordinate_dim_pqt (
  coordinate_id BIGINT,
  latitude DECIMAL(12,8),
  longitude DECIMAL(12,8),
  latitude_str string,
  longitude_str string
)
STORED as PARQUET
LOCATION 's3://sickdat-query-engine-talk/coordinate_dim_pqt/' 
tblproperties ("parquet.compression"="SNAPPY");

--select distinct latitude_numeric, longitude_numeric, coordinates.latitude, coordinates.longitude from openaq_pqt;
--select distinct latitude_numeric, longitude_numeric from openaq_pqt;
--select distinct coordinates.latitude, coordinates.longitude from openaq_pqt;
--
insert into coordinate_dim_pqt(coordinate_id, latitude, longitude, latitude_str, longitude_str)	
with coordinates as (
	select distinct latitude_numeric, longitude_numeric, coordinates.latitude as latitude_str, coordinates.longitude as longitude_str from openaq_pqt
) 
select 
	row_number() over (order by latitude_str desc, longitude_str desc) 
	 as coordinate_id, latitude_numeric, longitude_numeric, latitude_str, longitude_str	
	from coordinates;

select * from coordinate_dim_pqt;

drop table if exists date_dim_pqt;
CREATE TABLE date_dim_pqt
WITH (
      format = 'Parquet',
      external_location = 's3://sickdat-query-engine-talk/date_dim_pqt/',
      parquet_compression = 'SNAPPY'
	)
AS SELECT *
FROM date_dim;


-- averaging period, location (58211689) source
--
-- DOCO: Join all dimension tables back to fact table by business keys to related surrogate keys 
-- Outcome will be more standard fact and dimension tables
--
create or replace view openaq_fact_view as
select 
openaq_pqt.value as value,
openaq_pqt.unit as unit,
openaq_pqt.coordinates.latitude as latitude,
openaq_pqt.coordinates.longitude as longitude,
openaq_pqt.mobile as mobile,
pollutant_dim_pqt.pollutant_id as pollutant_id,
date_dim_pqt.date_id as date_id,
location_dim_pqt.location_id as location_id,
coordinate_dim_pqt.coordinate_id as coordinate_id,
averaging_period_dim_pqt.averaging_period_id as averaging_period_id,
source_dim_pqt.source_id as source_id,
cast(year(from_iso8601_timestamp(openaq_pqt.date.utc)) as int) as date_year,
cast(month(from_iso8601_timestamp(openaq_pqt.date.utc)) as int) as date_month
from openaq_pqt
-- all joins here will be on business keys (the reverse of what we usually report on)
left outer join pollutant_dim_pqt
 on openaq_pqt.parameter = pollutant_dim_pqt.pollutant_parameter
 left outer join date_dim_pqt
 on substr(openaq_pqt.date.utc,1,10) = date_dim_pqt.full_date
 left outer join location_dim_pqt
 on openaq_pqt.location = location_dim_pqt.location
  and openaq_pqt.city = location_dim_pqt.city
  and openaq_pqt.country = location_dim_pqt.country
 left outer join averaging_period_dim_pqt 
 on openaq_pqt.averagingperiod.unit = averaging_period_dim_pqt.unit
  and openaq_pqt.averagingperiod.value = averaging_period_dim_pqt.period_value
 left outer join source_dim_pqt
 on (openaq_pqt.sourcename = source_dim_pqt.sourcename
  and openaq_pqt.sourcetype = source_dim_pqt.sourcetype) 
  or (openaq_pqt.sourcename = source_dim_pqt.sourcename)
 left outer join coordinate_dim_pqt
 on openaq_pqt.coordinates.latitude = coordinate_dim_pqt.latitude_str
  and openaq_pqt.coordinates.longitude = coordinate_dim_pqt.longitude_str;
  
-- DOCO: Create Partitioned Parquet version of the fact view defined above
drop table if exists openaq_fact_pqt_part;
CREATE TABLE openaq_fact_pqt_part
WITH (
      format = 'Parquet',
      external_location = 's3://sickdat-query-engine-talk/openaq_fact_pqt_part/',
      parquet_compression = 'SNAPPY',
      partitioned_by = ARRAY['date_year','date_month'] 
	)
AS SELECT *
FROM openaq_fact_view
where openaq_fact_view.date_year>2010 and 
(date_id is not null and location_id is not null and pollutant_id is not null
	and source_id is not null and coordinate_id is not null and averaging_period_id is not null);

select count(*) from openaq_fact_pqt_part; 

-- DOCO: Had to add a few extra rows as an insert as Athena supports a Maximum of 100 partitions / CTAS
insert INTO openaq_fact_pqt_part
SELECT *
FROM openaq_fact_view
where openaq_fact_view.date_year<=2010 and 
(date_id is not null and location_id is not null and pollutant_id is not null
	and source_id is not null and coordinate_id is not null and averaging_period_id is not null);

--select count(*) from openaq_fact_pqt_part; 
--
--select count(*) as date_id_null_count from openaq_fact_pqt_part where date_id is null;
--select count(*) as location_id_null_count from openaq_fact_pqt_part where location_id is null;
--select count(*) as pollutant_id_null_count from openaq_fact_pqt_part where pollutant_id is null;
--select count(*) as source_id_null_count from openaq_fact_pqt_part where source_id is null; -- 14880271
--select count(*) as coordinate_id_null_count from openaq_fact_pqt_part where coordinate_id is null; -- 33935693, these do not have coordinates
--select count(*) as averaging_period_id_null_count from openaq_fact_pqt_part where averaging_period_id is null; -- 40503523, these do not have value
--
--select count(*) as expectedNoNullCount from openaq_fact_pqt_part where 
--	date_id is null or location_id is null or pollutant_id is null;
--
--select count(*) knownNullCount from openaq_fact_pqt_part where 
--	date_id is null or location_id is null or pollutant_id is null
--	or source_id is null or coordinate_id is null or averaging_period_id is null; -- 85763198 / 1143450757
--
--select count(*) from openaq_fact_pqt_part;
--
--select count(*) knownGoodCount from openaq_fact_pqt_part where 
--	date_id is not null and location_id is not null and pollutant_id is not null
--	and source_id is not null and coordinate_id is not null and averaging_period_id is not null; -- 85763198 / 1143450757

se
