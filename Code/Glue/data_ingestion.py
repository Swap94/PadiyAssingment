import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace, col, when
from awsglue.dynamicframe import DynamicFrame
import logging
import argparse
from datetime import datetime, timedelta, timezone
import time


def __get_logger(log_level=logging.INFO, process_name="%(name)s"):
    """
        INPUT  : Logging level : [INFO, DEBUG, WARNING, ERROR] , process_name : Name of the process
        OUTPUT : Return logging object
        @desc  : Function for basic Job logging
    """
    logging.basicConfig(level=log_level,
                        format=f'%(asctime)s - {process_name} - %(process)d - %(levelname)s - %(message)s')
    return logging

def __get_parameter(param_key):
    """
        INPUT  : Parameter Store Key
        OUTPUT : Parmeter Value
        @desc  : Function for to retrieve parameter from AWS Parameter store
    """
    ssm = boto3.client('ssm')
    response = ssm.get_parameters(
        Names=[
            param_key,
        ],
        WithDecryption=True
    )
    return response['Parameters'][0]['Value']

def __parameter_initialization(dt, parameter_prefix):
    """
        INPUT  : Job RUN Date(YYYYMMDDHH), Parameter Store Prefix
        OUTPUT : Parameter required to load data into table [connection_options, src_db_name, src_tbl_name, predicate, columns_to_drop, connection_type]
        @desc  : Function for to initialize parameters used to load data into db table
    """
    
    db_url = __get_parameter(parameter_prefix + "db_url")
    dbtable = __get_parameter(parameter_prefix + "dbtable")
    user = __get_parameter(parameter_prefix + "user")
    password = __get_parameter(parameter_prefix + "password")
    customJdbcDriverS3Path = __get_parameter(parameter_prefix + "customJdbcDriverS3Path")
    customJdbcDriverClassName = __get_parameter(parameter_prefix + "customJdbcDriverClassName")
    src_db_name = __get_parameter(parameter_prefix + "src_db_name")
    src_tbl_name = __get_parameter(parameter_prefix + "src_tbl_name")
    yyyy=str(dt.year)
    mm=f'{dt.month:02d}'
    dd=f'{dt.day:02d}'
    hh=f'{dt.hour:02d}'
    predicate = "(yyyy=='" + str(yyyy) + "' and mm=='" + str(mm) + "' and dd=='" + str(dd) + "' and hh=='" + str(hh) + "')"
    columns_to_drop = __get_parameter(parameter_prefix + "columns_to_drop")
    connection_type = __get_parameter(parameter_prefix + "connection_type")
    s3_athena_result = __get_parameter(parameter_prefix + "athena_bucket")
    source_files_bucket = __get_parameter(parameter_prefix + "source_files_bucket")
    
    connection_options = {
        "url": db_url,
        "dbtable": dbtable,
        "user": user,
        "password": password,
        "customJdbcDriverS3Path": customJdbcDriverS3Path,
        "customJdbcDriverClassName": customJdbcDriverClassName}
        
    return connection_options, src_db_name, src_tbl_name, predicate, columns_to_drop, connection_type, s3_athena_result, yyyy, mm, dd, hh, source_files_bucket

def __update_glue_table_partition(src_db_name, src_tbl_name, s3_athena_result, f_logging, yyyy, mm, dd, hh, source_files_bucket):
    """
        INPUT  : Source Database, Source Table, Athena Result Bucket, Log Object
        OUTPUT : NO OUTPUT
        @desc  : Function for to initialize parameters used to load data into db table
    """
    
    athena = boto3.client('athena')
    partition_cond = "(YYYY ='" + yyyy + "', MM ='" + mm + "', DD ='"+dd+ "', HH ='" + hh + "')"
    file_location = source_files_bucket + "yyyy="+ yyyy + "/mm=" + mm + "/dd=" + dd + "/hh=" + hh 
    query = f"ALTER TABLE {src_db_name}.{src_tbl_name} ADD PARTITION {partition_cond} LOCATION '{file_location}'"
    
    f_logging.log(logging.INFO, f"Update Query : {query}")
    
    st_response = athena.start_query_execution(
         QueryString = query,
         ResultConfiguration = {
             'OutputLocation': s3_athena_result
         })
         
    f_logging.log(logging.INFO, f"Update Partition Query Execution ID : {st_response['QueryExecutionId']}")
    
    while True:
        gt_response = athena.get_query_execution(
                      QueryExecutionId=st_response['QueryExecutionId']
                      )
        status = gt_response['QueryExecution']['Status']['State']              
        if status not in ('RUNNING', 'QUEUED'):
            f_logging.log(logging.INFO, f"Query Status : {status}")
            break
        else:
            f_logging.log(logging.INFO, f"Query Status : {status}")
            time.sleep(3)

    if status == "SUCCEEDED":
        return True
    elif status == "FAILED":
        error = gt_response['QueryExecution']['Status']['StateChangeReason']  
        
        if 'Partition already exists' in error:
            f_logging.log(logging.INFO, f"Query ERROR : Partition already exists")
            return True
        else:
            return False
    elif status == "CANCELLED":
        return False
        
def __read_source_data(src_db_name, src_tbl_name, predicate):
    """
        INPUT  : Source Database, Source Table, Partition Condition(1 - Hour Data)
        OUTPUT : Return AWS Data Catalog DynamicFrame
        @desc  : Read data from AWS Glue Catalog
    """
    try:
        datasource0 = glueContext.create_dynamic_frame.from_catalog \
                      (database = src_db_name, table_name = src_tbl_name, transformation_ctx = "datasource0", push_down_predicate = predicate)
        return datasource0
    except:
        return None

def __transform_data(datasource0, columns_to_drop):
    """
        INPUT  : AWS Data Catalog DynamicFrame
        OUTPUT : Transformed DynamicFrame 
        @desc  : Function for to tranform data before loading into db table
    """
    
    datasource0_reduced = datasource0.drop_fields([columns_to_drop])
        
    datasource0_reduced_sdf = datasource0.toDF()
    
    transform_sdf = datasource0_reduced_sdf.withColumn('SeriousDlqin2yrs', when(col('SeriousDlqin2yrs').contains('NA'), None).otherwise(col('SeriousDlqin2yrs')))\
                                            .withColumn('RevolvingUtilizationOfUnsecuredLines', when(col('RevolvingUtilizationOfUnsecuredLines').contains('NA'), None).otherwise(col('RevolvingUtilizationOfUnsecuredLines')))\
                                            .withColumn('age', when(col('age').contains('NA'), None).otherwise(col('age')))\
                                            .withColumn('NumberOfTime30-59DaysPastDueNotWorse', when(col('NumberOfTime30-59DaysPastDueNotWorse').contains('NA'), None).otherwise(col('NumberOfTime30-59DaysPastDueNotWorse')))\
                                            .withColumn('DebtRatio', when(col('DebtRatio').contains('NA'), None).otherwise(col('DebtRatio')))\
                                            .withColumn('MonthlyIncome', when(col('MonthlyIncome').contains('NA'), None).otherwise(col('MonthlyIncome')))\
                                            .withColumn('NumberOfOpenCreditLinesAndLoans', when(col('NumberOfOpenCreditLinesAndLoans').contains('NA'), None).otherwise(col('NumberOfOpenCreditLinesAndLoans')))\
                                            .withColumn('NumberOfTimes90DaysLate', when(col('NumberOfTimes90DaysLate').contains('NA'), None).otherwise(col('NumberOfTimes90DaysLate')))\
                                            .withColumn('NumberRealEstateLoansOrLines', when(col('NumberRealEstateLoansOrLines').contains('NA'), None).otherwise(col('NumberRealEstateLoansOrLines')))\
                                            .withColumn('NumberOfTime60-89DaysPastDueNotWorse', when(col('NumberOfTime60-89DaysPastDueNotWorse').contains('NA'), None).otherwise(col('NumberOfTime60-89DaysPastDueNotWorse')))\
                                            .withColumn('NumberOfDependents', when(col('NumberOfDependents').contains('NA'), None).otherwise(col('NumberOfDependents')))\
                                            .select('SeriousDlqin2yrs',\
                                                    'RevolvingUtilizationOfUnsecuredLines',\
                                                    'age',\
                                                    'NumberOfTime30-59DaysPastDueNotWorse',\
                                                    'DebtRatio',\
                                                    'MonthlyIncome',\
                                                    'NumberOfOpenCreditLinesAndLoans',\
                                                    'NumberOfTimes90DaysLate',\
                                                    'NumberRealEstateLoansOrLines',\
                                                    'NumberOfTime60-89DaysPastDueNotWorse',\
                                                    'NumberOfDependents'\
                                                    )
                                                    
    
    
    transform_dyf = DynamicFrame.fromDF(transform_sdf, glueContext, "nested")
    
    return transform_dyf
    
def __data_load_db(transform_dyf, connection_options, connection_type): 
    """
        INPUT  : Transformed Dynamic Frame, MySql Connection Setting, Connection Type
        OUTPUT : NO OUTPUT
        @desc  : Function for to load data into MySQL table using JDBC driver
    """
    glueContext.write_from_options(frame_or_dfc=transform_dyf, connection_type=connection_type, connection_options=connection_options)

if __name__ == "__main__" :
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    f_logging = __get_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument('--date_time', type=str)
    parser.add_argument('--parameter_prefix', type=str)
    args, unknown = parser.parse_known_args()
    
    f_logging.log(logging.INFO, f"Job Parameters : {args.date_time}, {args.parameter_prefix}")
    
    if args.date_time is None:
        cur_date = datetime.now(timezone.utc)
    else:
        cur_date = datetime.strptime(args.date_time,'%Y%m%d%H')
    
    if args.parameter_prefix is not None:
        parameter_prefix = args.parameter_prefix
    else:
        f_logging.log(logging.ERROR, f"Specify Parameter Store Prefix")
        exit(1)
    
    f_logging.log(logging.INFO, f"Current_Date : {args.date_time} Parameter Store Prefix : {args.parameter_prefix}")
    
    connection_options, src_db_name, src_tbl_name, predicate, columns_to_drop, connection_type, s3_athena_result, yyyy, mm, dd, hh, source_files_bucket = __parameter_initialization(cur_date, parameter_prefix)
    
    f_logging.log(logging.INFO, \
                  f"Parameter Initialization Completed!")
    
    status = __update_glue_table_partition(src_db_name, src_tbl_name, s3_athena_result, f_logging, yyyy, mm, dd, hh, source_files_bucket)
    
    if status:
            
        f_logging.log(logging.INFO, \
                      f"Updated Partition for {src_db_name}.{src_tbl_name} with {predicate}!")
                      
        datasource0 = __read_source_data(src_db_name, src_tbl_name, predicate)
        
        f_logging.log(logging.INFO, f"Source Table {src_db_name}.{src_tbl_name} Created!")
        
        if datasource0 is not None:
            
            if datasource0.toDF().count() != 0:
                transform_dyf = __transform_data(datasource0, columns_to_drop)
                
                f_logging.log(logging.INFO, f"Source Table {src_db_name}.{src_tbl_name} Transformation Completed!")
                
                __data_load_db(transform_dyf, connection_options, connection_type)
                
                f_logging.log(logging.INFO, f"Data Load Completed!")
                f_logging.log(logging.INFO, f"Total Count Loaded {transform_dyf.toDF().count()}!")
    
            else:
                f_logging.log(logging.WARNING, f"Source Table {src_db_name}.{src_tbl_name} does not contain data for {predicate}!")
        else:
            f_logging.log(logging.WARNING, f"Source Table {src_db_name}.{src_tbl_name} not found!")            
    else:
        f_logging.log(logging.WARNING, \
                      f"Failed Update Partition for {src_db_name}.{src_tbl_name} with {predicate} Failed!")