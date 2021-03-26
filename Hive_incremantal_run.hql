--hive --hivevar load_date_in='20210314' -f Hive_incremental_run.hql
--use the database
use telecom;

--Set spark as an execution engine
set hive.execution.engine=tez;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

add jar /home/cloudera/Downloads/csv-serde-0.9.1.jar;

--load into stage
Insert overwrite table telecom_stage
partition (load_date)
select 
ID ,
customerID  ,
customerName  ,
gender  ,
pinCode ,
seniorCitizen ,
partner  ,
tenure ,
phoneService  ,
multipleLines  ,
internetService  ,
onlineSecurity  ,
deviceProtection  ,
streamingTV  ,
streamingMovies  ,
contract  ,
paperlessBilling  ,
paymentMethod,
monthlyCharges,
totalCharges,
current_timestamp as created_date_time,
current_timestamp as last_modified_datetime,
load_date
from telecom_raw where load_date = '${load_date_in}';

--1) create a view - create view at only at first incremental run. Later, we can use the view directly
create view if not exists incremental_data as
select t1.ID ,
t1.customerID  ,
t1.customerName  ,
t1.gender  ,
t1.pinCode ,
t1.seniorCitizen ,
t1.partner  ,
t1.tenure ,
t1.phoneService  ,
t1.multipleLines  ,
t1.internetService  ,
t1.onlineSecurity  ,
t1.deviceProtection  ,
t1.streamingTV  ,
t1.streamingMovies  ,
t1.contract  ,
t1.paperlessBilling  ,
t1.paymentMethod,
t1.monthlyCharges,
t1.totalCharges,
t1.created_date_time,
t1.last_modified_datetime,
t1.load_date from
(select * from telecom_stage) t1
join
(select id,max(load_date) as load_date from (select * from telecom_stage) t2 group by id) s on t1.id=s.id and t1.load_date=s.load_date;

--load into main with updated and new records

INSERT overwrite table telecom
partition(load_date)
select * from incremental_data;