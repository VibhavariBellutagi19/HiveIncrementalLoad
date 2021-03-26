-- script command -> % hive -f test.hql
--create a database - 

create database telecom;

--use the database
use telecom;


--Add the jar opencsv to escape ',' in the column review_text

add jar /home/cloudera/Downloads/csv-serde-0.9.1.jar;

--or add jar under  /usr/lib/hive/lib for permanent availibity of jar

--Create a hive external table with load date as partition, to load the data date wise
Create external table IF NOT Exists telecom_raw(
ID int,
customerID string,
customerName string,
gender string,
pinCode int,
seniorCitizen tinyint,
partner string,
tenure int,
phoneService string,
multipleLines string,
internetService string,
onlineSecurity string,
deviceProtection string,
streamingTV string,
streamingMovies string,
contract string,
paperlessBilling string,
paymentMethod string,
monthlyCharges float,
totalCharges float,
load_date string
 )
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
location '/user/cloudera/miniprojs/hive/raw/'
tblproperties("skip.header.line.count"="1","serialization.null.format"="");

--create stage

Create external table IF NOT Exists telecom_stage(
ID int,
customerID string,
customerName string,
gender string,
pinCode int,
seniorCitizen tinyint,
partner string,
tenure int,
phoneService string,
multipleLines string,
internetService string,
onlineSecurity string,
deviceProtection string,
streamingTV string,
streamingMovies string,
contract string,
paperlessBilling string,
paymentMethod string,
monthlyCharges float,
totalCharges float,
created_date_time timestamp,
last_modified_datetime timestamp)
partitioned by (load_date string)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
location '/user/cloudera/miniprojs/hive/stage/';

--create main table

Create external table IF NOT Exists telecom(
ID int,
customerID string,
customerName string,
gender string,
pinCode int,
seniorCitizen tinyint,
partner string,
tenure int,
phoneService string,
multipleLines string,
internetService string,
onlineSecurity string,
deviceProtection string,
streamingTV string,
streamingMovies string,
contract string,
paperlessBilling string,
paymentMethod string,
monthlyCharges float,
totalCharges float,
created_date_time timestamp,
last_modified_datetime timestamp)
partitioned by (load_date string)
stored as orc
location '/user/cloudera/miniprojs/hive/main/';