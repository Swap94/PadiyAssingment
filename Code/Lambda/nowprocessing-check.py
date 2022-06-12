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
    parameter_store_prefix = event['parameter_store_prefix']

    print(f"Enviornment String {event['parameter_store_prefix']}")

    queue_s3_bucket = get_parameters(
        parameter_store_prefix + "queue_files_bucket")
    key = get_parameters(parameter_store_prefix + "etl_prefix")
    file_name = get_parameters(
        parameter_store_prefix + "nowprocessing_file")

    ########################################################################
    #   Check nowprocessing file availability                              #
    ########################################################################
    objs = s3.list_objects_v2(Bucket=queue_s3_bucket, Prefix=key+file_name)

    process_status = 0

    if objs['KeyCount'] == 1:
        process_status = 9

    return {"process_status": process_status}
