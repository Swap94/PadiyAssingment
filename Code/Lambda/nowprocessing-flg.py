import json
import boto3
from datetime import datetime, timedelta, timezone


def get_parameters(param_key):
    """
    @desc: Get Parameters from Parameter Store

    """
    ssm = boto3.client('ssm')
    response = ssm.get_parameters(
        Names=[
            param_key,
        ],
        WithDecryption=True
    )
    return response['Parameters'][0]['Value']


def lambda_handler(event, context):
    ########################################################################
    #   Get Parameters                                                     #
    ########################################################################
    s3 = boto3.client('s3')
    is_parallel = 0
    timestamp = event['timestamp']
    process_status = event['process_status']
    parameter_store_prefix = event['parameter_store_prefix']
    print(timestamp, process_status, parameter_store_prefix)
    queue_s3_bucket = get_parameters(
        parameter_store_prefix + 'queue_files_bucket')
    key = get_parameters(parameter_store_prefix + 'etl_prefix')
    queue_key = get_parameters(parameter_store_prefix + 'queue_files_prefix')
    now_processing = get_parameters(
        parameter_store_prefix + 'nowprocessing_file')

    ########################################################################
    #   Create User Defined Variables                                      #
    ########################################################################
    file_name = key + now_processing
    queque_file_name = queue_key + timestamp

    ########################################################################
    #   Create or delete now processing file  as per process status        #
    ########################################################################
    if process_status == 0:
        response = s3.list_objects_v2(Bucket=queue_s3_bucket, Prefix=file_name)
        print(f"Response:{response.keys()}")
        
        ## Parallel Job Check Logic ####
        
        if 'Contents' in response.keys():
            contents = response['Contents']
            for obj in contents:
                if obj['Key'] == file_name:
                    print(f"S3 Repsonse: {obj['Key']}")
                    is_parallel = 9
                    print(f"Warning: Another program is running for {timestamp}")
        
        if is_parallel == 0:
            s3.put_object(Bucket=queue_s3_bucket, Key=file_name, Body=timestamp)

    elif process_status == 1:
        s3.delete_object(Bucket=queue_s3_bucket, Key=file_name)
        s3.delete_object(Bucket=queue_s3_bucket, Key=queque_file_name)

    return {"timestamp": timestamp, "is_parallel":is_parallel}