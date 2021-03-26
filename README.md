# HiveIncrementalLoad

Hive Incremental load - This is the mini project which explains the incremental load in hive. 
There are three files - 
1. Hive_table_creation.hql
2. Hive_inital_run.hql
3. Hive_incremantal_run.hql

Brief explaination - 

1. Hive_table_creation.hql - This file creates the external hive table. There are 3 tables - Raw table, Stage table and target table.
2. Hive_inital_run.hql - Initial run or history run, which will be first time load.
3. Hive_incremantal_run.hql - Incremental run, which will have delta data for daily runs.
