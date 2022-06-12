CREATE DATABASE paidyassignment;


CREATE EXTERNAL TABLE `paidyassingment_source_files`
(
  `row_no` bigint, 
  `SeriousDlqin2yrs` string, 
  `RevolvingUtilizationOfUnsecuredLines` string, 
  `age` string, 
  `NumberOfTime30-59DaysPastDueNotWorse` string, 
  `DebtRatio` string, 
  `MonthlyIncome` string, 
  `NumberOfOpenCreditLinesAndLoans` string, 
  `NumberOfTimes90DaysLate` string, 
  `NumberRealEstateLoansOrLines` string, 
  `NumberOfTime60-89DaysPastDueNotWorse` string, 
  `NumberOfDependents` string
  )
PARTITIONED BY ( 
  `yyyy` string, 
  `mm` string, 
  `dd` string, 
  `hh` string)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
 WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '\"',
   'escapeChar' = '\\'
   )
STORED AS TEXTFILE
LOCATION '' -- Place S3 file location eg. s3://paidyassignment-aws-glue/paidyassingment_source_files/
TBLPROPERTIES (
  'skip.header.line.count'='1');