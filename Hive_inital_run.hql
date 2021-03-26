--% hive --hivevar load_date_in='19000101' -f Hive_inital_run.hql

--use the database
use telecom;

--Set spark as an execution engine
set hive.execution.engine=spark;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

--load the data stage table with default value of createdatetime,modifiedtime for particular id.

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
from telecom_raw;

--load the data to main table
Insert overwrite table telecom
partition (load_date)
select * from telecom_stage where load_date=${load_date_in};