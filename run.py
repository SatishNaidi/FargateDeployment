import os
import logging
import boto3
import pprint
import csv
from datetime import datetime, date
import json
import sys
from time import sleep
from botocore.exceptions import ClientError
from xlsxwriter.workbook import Workbook

# Setting up the logger
try:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime) -24s:%(levelname) -8s:%(funcName)s(%(lineno)s)>> %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    log_level = "DEBUG"
    log_level = os.environ['log_level']
    if log_level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        level_obj = logging.getLevelName(log_level)
except KeyError:
    logger.warning("log_level Environment Variable Doesn't exist")
    log_level = "DEBUG"
    level_obj = logging.getLevelName(log_level)
logger.debug('Log Level Set to : DEBUG')
logger.setLevel(level_obj)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def pp(item):
    """Pretty Prints the output based on Content"""
    pprint.pprint(item)


def _flatten_json(y):
    """
    Flattens json, ex:
    {
    "id":
        {
        "name":"value"
        }
    } will be converted to { "id_name":"value" }
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def format_nested_keys(input_dict):
    all_keys = list(input_dict.keys())
    for each_key in all_keys:
        if each_key == "Tags":
            for value in input_dict["Tags"]:
                if value["Key"] == "Name":
                    input_dict["Name"] = value["Value"]
                else:
                    input_dict["Tag_" + value["Key"]] = value["Value"]
            del input_dict["Tags"]
        elif each_key == "SecurityGroups":
            attr = []
            for value in input_dict["SecurityGroups"]:
                attr.append(value["GroupName"] + ":" + value["GroupId"])
            input_dict["SecurityGroups"] = ",".join(attr)
        elif each_key == "IamInstanceProfile":
            input_dict["IamInstanceProfile"] = input_dict["IamInstanceProfile"]["Arn"]
        elif each_key == "State":
            input_dict["State"] = input_dict["State"]["Name"]
        elif each_key == "LaunchTime":
            input_dict["LastBootTime"] = input_dict["LaunchTime"]
            del input_dict["LaunchTime"]
        else:
            pass
    return _flatten_json(input_dict)


def gather_ec2_instance_info(next_token):
    all_instances_info = []
    ec2_client = boto3.client('ec2', region_name="us-east-1")
    logger.debug("{}: EC2 Client Connection Object Created".format(ec2_client))
    response = ec2_client.describe_instances(MaxResults=30, NextToken=next_token)
    next_token = response.get("NextToken")
    logger.debug(f"NextToken: {next_token}")
    for reservation in response.get("Reservations"):
        for instance in reservation.get("Instances"):
            all_instances_info.append(instance)
    logger.debug(f" Len of current Instances : {len(all_instances_info)}")
    return all_instances_info, next_token


def divide_chunks(items_list, n):
    # looping till length l
    for i in range(0, len(items_list), n):
        yield items_list[i:i + n]


def gather_instance_patch_states(ssm_client, ec2_instance_ids):
    logger.info("Started")
    pages = ssm_client.get_paginator('describe_instance_patch_states')
    chunked_ids = list(divide_chunks(ec2_instance_ids, 40))
    instance_patches = []
    for each_chunk in chunked_ids:
        logger.debug(f"Current Query InstanceIds: {each_chunk}")
        for page in pages.paginate(InstanceIds=each_chunk):
            instance_patches.extend(page["InstancePatchStates"])
    return instance_patches


def gather_instance_patch_info(ssm_client):
    logger.debug("Started")
    pages = ssm_client.get_paginator('describe_instance_information')
    all_instances = []
    for page in pages.paginate():
        all_instances.extend(page.get("InstanceInformationList", []))
    logger.debug(f"Len all_instances: {len(all_instances)}")
    return all_instances


def detailed_instance_patch_report(instance_id, state, next_token=''):
    ssm_client = boto3.client('ssm', region_name="us-east-1")
    logger.debug(f"Running for state: {instance_id}, Instance: {state}")
    try:
        instance_patch_report = []
        # paginator = ssm_client.get_paginator('describe_instance_patches')

        if next_token == '':
            result = ssm_client.describe_instance_patches(
                InstanceId=instance_id,
                Filters=[{'Key': 'State', 'Values': [state]}],
                MaxResults=40
            )
        else:
            result = ssm_client.describe_instance_patches(
                InstanceId=instance_id,
                Filters=[{'Key': 'State', 'Values': [state]}],
                NextToken=next_token,
                MaxResults=40
            )
        items = []
        next_token = result.get("NextToken")
        items.extend(result.get("Patches", []))
        items = json.loads(json.dumps(items, default=json_serial))
        for item in items:
            item["InstanceId"] = instance_id

        # instance_patch_report = [dict(item, InstanceId=instance_id) for item in items]
        instance_patch_report = json.loads(json.dumps(items, default=json_serial))
    except ClientError as cl_err:
        next_token = None
        logger.warning(f'Error: {cl_err}')
    except Exception as outErr:
        next_token = None
        logger.warning(outErr)
    logger.info(f"Len master_patch_report: {len(instance_patch_report)}")
    return instance_patch_report, next_token


def filter_needed_fields(input_dict, filter_keys):
    final_out = []
    for each_instance in input_dict:
        json_formatted = json.loads(json.dumps(each_instance, default=json_serial))
        input_dict_keys = json_formatted.keys()
        each_instance_dict = {}
        for each_key in filter_keys:
            if each_key in input_dict_keys:
                each_instance_dict[each_key] = json_formatted[each_key]
        final_out.append(format_nested_keys(each_instance_dict))
    return final_out


def write_to_csv(filename, list_of_dict):
    """
    :param filename:
    :param list_of_dict:
    :return:
    """
    # Making sure to write to /tmp dir if running on AWS Lambda other wise to current dir
    # if __name__ == "__main__":
    if sys.platform.startswith("win"):
        if not os.path.exists("tmp/"):
            os.mkdir("tmp")
        filename = "tmp/" + filename
    else:
        filename = "/tmp/" + filename
    logger.info("Writing CSV File : {} ".format(filename))

    json_serialized = json.loads(json.dumps(list_of_dict, default=json_serial))
    columns = []
    all_rows = []
    for each_item in json_serialized:
        row = ["" for col in columns]
        for key, value in each_item.items():
            try:
                index = columns.index(key)
            except ValueError:
                # this column hasn't been seen before
                columns.append(key)
                row.append("")
                index = len(columns) - 1
            row[index] = value
        all_rows.append(row)
    with open(filename, "w", newline='') as csv_file:
        writer = csv.writer(csv_file)
        # first row is the headers
        writer.writerow(columns)
        # then, the rows
        writer.writerows(all_rows)
    logger.info("Successfully written CSV File: {}".format(filename))
    return filename


def convert_csv_to_excel(out_file_name, csv_list):
    """
    Converts all given CSV List of files to EXCEL
    """

    # if __name__ == "__main__":
    if sys.platform.startswith("win"):
        if not os.path.exists("tmp/"):
            os.mkdir("tmp")
        out_file_name = "tmp/" + out_file_name
    else:
        out_file_name = "/tmp/" + out_file_name

    logger.info(f"Excel File: {out_file_name}")
    try:
        workbook = Workbook(out_file_name)
        for each_csv_file in csv_list:
            sheet_name = os.path.splitext(os.path.basename(each_csv_file))[0]
            worksheet = workbook.add_worksheet(sheet_name)
            with open(each_csv_file, 'rt', encoding='utf8') as f:
                reader = csv.reader(f)
                for r, row in enumerate(reader):
                    for c, col in enumerate(row):
                        worksheet.write(r, c, col)
        workbook.close()
        logger.info(f"Successfully created file {out_file_name}")
        return out_file_name
    except Exception as err:
        logger.error(err)
        return False


def patch_base_line_names_to_ids(client, patch_baselines):
    try:
        logger.warning("Getting the Patch Baseline IDs for {}".format(patch_baselines))
        paginator = client.get_paginator('describe_patch_baselines')
        response_base_lines = {}
        response_iterator = paginator.paginate(
            Filters=[
                {
                    'Key': 'NAME_PREFIX',
                    'Values': patch_baselines
                }
            ]
        )
        for page in response_iterator:
            for each_item in page["BaselineIdentities"]:
                base_line_id = each_item["BaselineId"]
                base_line_name = each_item["BaselineName"]
                response_base_lines[base_line_id] = base_line_name
        logger.debug("PatchBaseline IDs : {}".format(response_base_lines))
        return response_base_lines
    except Exception as Err:
        logger.exception("Exception: {}".format(Err))
        return False


def get_effective_patches(client, patch_base_lines):
    logger.info(f"patch_base_lines: {patch_base_lines}")
    list_of_patches = []
    for pbid, pbname in patch_base_lines.items():
        try:
            patch_baseline_response = client.describe_effective_patches_for_patch_baseline(
                BaselineId=pbid
            )
            json_serialized = json.loads(json.dumps(patch_baseline_response, default=json_serial))
            for each_patch in json_serialized["EffectivePatches"]:
                new_item = each_patch["Patch"]
                new_item.update(each_patch["PatchStatus"])
                new_item.update({"PBName": pbname, "PBId": pbid})
                list_of_patches.append(new_item)
        except Exception as err:
            logger.warning(f"{err} for {pbname}:{pbid}")
    logger.debug(f"list_of_patches: {list_of_patches}")
    logger.info(f"Length of list_of_patches: {len(list_of_patches)}")
    return list_of_patches


def upload_file_s3(client, bucket_name, to_be_upload_filename):
    logger.debug(f"Bucket Name:{bucket_name}, LocalFile : {to_be_upload_filename}")
    only_filename = os.path.basename(to_be_upload_filename)
    try:
        res = client.upload_file(to_be_upload_filename, bucket_name, only_filename)
        logger.debug(res)
        return "File: " + only_filename + "Uploaded to bucket : " + bucket_name
    except Exception as err:
        logger.error(err)
        return err


def lambda_handler(event, context):
    """
    Default Handler
    """
    # Logging Priority EX: DEBUG will logs everything, while ERROR will only logs ERROR and CRITICAL
    # DEBUG, INFO, WARNING, ERROR, CRITICAL
    # Connection Objects
    ec2_client = boto3.client('ec2', region_name="us-east-1")
    logger.debug("{}: EC2 Client Connection Object Created".format(ec2_client))
    ssm_client = boto3.client('ssm', region_name="us-east-1")
    logger.debug("{}: SSM Client Connection Object Created".format(ssm_client))
    csvs_list = []

    try:
        patch_baselines = os.environ['patch_baselines'].split(",")
    except KeyError:
        logger.warning("patch_baselines Environment Variable Doesn't exist")
        patch_baselines = ["WindowsApprovedPatches", "AmazonLinuxApprovedPatches", "LinuxApprovedPatches"]
    logger.info("patch_baselines = {}".format(patch_baselines))

    try:
        bucket_name = os.environ['bucket_name']
    except KeyError:
        logger.warning("Env Variable 'bucket_name' doesn't exits")
        bucket_name = "2ftv-ssm-logs-42212-s3"
    logger.info("Bucket for writing logs bucket_name = {}".format(bucket_name))

    # PatchBaselines Report
    response_patch_base_lines = patch_base_line_names_to_ids(ssm_client, patch_baselines)
    list_of_patches = get_effective_patches(ssm_client, response_patch_base_lines)
    csvs_list.append(write_to_csv("PatchBaseLineReport.csv", list_of_patches))
    # EC2Report
    field_names = ['InstanceId', 'State', 'IamInstanceProfile', 'Tags', 'LaunchTime']
    # ec2_info = gather_ec2_instance_info()
    next_token = ''
    ec2_info = []
    while next_token is not None:
        result_instances, next_token = gather_ec2_instance_info(next_token)
        ec2_info.extend(result_instances)

    logger.debug(f"Total EC2 Instances Count : {len(ec2_info)}")
    required_info = filter_needed_fields(ec2_info, field_names)
    required_info_instance_ids = {item["InstanceId"]: item for item in required_info}

    # Instance Patch State
    instance_patch_state = gather_instance_patch_states(ssm_client, list(required_info_instance_ids.keys()))
    instance_patch_state = {each_item["InstanceId"]: each_item for each_item in instance_patch_state}

    # Instance Patch Info
    instance_patch_info = gather_instance_patch_info(ssm_client)
    instance_patch_info = {each_item["InstanceId"]: each_item for each_item in instance_patch_info}

    # Detailed Instance Patch Report
    # instance_patch_report = detailed_instance_patch_report(ssm_client, required_info_instance_ids)

    all_instance_patch_report = []
    try:
        for each_instance in required_info_instance_ids.keys():
            for each_state in ["Installed", "Missing", "Failed"]:
                next_token = ''
                while next_token is not None:
                    result_set, next_token = detailed_instance_patch_report(each_instance, each_state, next_token)
                    all_instance_patch_report.extend(result_set)
    except Exception as err:
        print(err)

    for each_instance in all_instance_patch_report:
        if each_instance["InstanceId"] in required_info_instance_ids:
            each_instance["Name"] = required_info_instance_ids[each_instance["InstanceId"]].get("Name", "NA")
            each_instance["RunState"] = required_info_instance_ids[each_instance["InstanceId"]]["State"]

    csvs_list.append(write_to_csv("EC2PatchReport.csv", all_instance_patch_report))

    # Consolidating EC2 Report, Patch State Report and Instance Patch Info
    for each_ec2 in required_info:
        each_ec2.update(instance_patch_state.get(each_ec2["InstanceId"], {}))
        each_ec2.update(instance_patch_info.get(each_ec2["InstanceId"], {}))

    csvs_list.append(write_to_csv("EC2Report.csv", required_info))
    s3_client = boto3.client("s3", region_name="us-east-1")
    logger.debug("{}: S3 Client Connection Object Created".format(s3_client))

    current_date = datetime.now()
    dt_string = current_date.strftime("%d_%b_%Y_%H_%M")
    consolidated_report_name = "ConsolidatedReport_" + dt_string + ".xlsx"
    xls_file = convert_csv_to_excel(consolidated_report_name, csvs_list)
    logger.debug(xls_file)
    final_response = {}
    try:
        result = upload_file_s3(s3_client, bucket_name, xls_file)
        logger.info(result)
        final_response[os.path.basename(xls_file)] = result
    except Exception as err:
        logger.error(err)
        logger.error("Error in Uploading file : " + xls_file)
        final_response[os.path.basename(xls_file)] = "Upload Failed"
    return {
        'statusCode': 200,
        'body': final_response
    }


if __name__ == "__main__":
    pprint.pprint(lambda_handler({}, {}))
