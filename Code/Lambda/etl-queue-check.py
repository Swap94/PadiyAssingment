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

    print(f"Environment Variable {parameter_store_prefix}")

    queue_s3_bucket = get_parameters(
        parameter_store_prefix + 'queue_files_bucket')
    key = get_parameters(
        parameter_store_prefix + 'queue_files_prefix')

    ########################################################################
    #   Creating List of queue bucket and picking latest queue             #
    ########################################################################
    objs = s3.list_objects(Bucket=queue_s3_bucket, Prefix=key)

    flag_list = []

    if 'Contents' in objs:
        for content in objs['Contents']:
            keyn = content['Key']
            pos = keyn.rfind('/') + 1
            prt = keyn[pos:]
            flag_list.append(prt)

    if '' in flag_list:
        flag_list.remove('')

    if len(flag_list) == 0:
        timestamp = ""
        process_status = 9

    else:
        flag_list.sort()
        timestamp = flag_list[0]
        process_status = 0

    return {"timestamp": timestamp, "process_status": process_status}
