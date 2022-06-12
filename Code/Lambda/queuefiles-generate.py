import json
import boto3
from datetime import datetime, timedelta, timezone
import os


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

    print(f"Event Time : {event['time']}")
    print(f"Event parameter_store_prefix: {os.environ['parameter_store_prefix']}")
    s3 = boto3.resource('s3')
    parameter_store_prefix = os.environ['parameter_store_prefix']

    queue_s3_bucket = get_parameters(
        parameter_store_prefix+"queue_files_bucket")
    key = get_parameters(
        parameter_store_prefix+"queue_files_prefix")

    print(f"Queue S3 Bucket :{queue_s3_bucket}, key:{key}")


    dttm = datetime.strptime(event['time'], '%Y-%m-%dT%H:%M:%SZ')
    now = dttm
    data_load_time = now.strftime("%Y%m%d%H")

    print("Current Time : ", data_load_time)

    ########################################################################
    #   Create Queue File(YYYYMMDDHH)                                      #
    ########################################################################
    obj = s3.Object(queue_s3_bucket, key+data_load_time)
    obj.put()

    print("Queue Bucket =", queue_s3_bucket)
    print("Current Time =", data_load_time)