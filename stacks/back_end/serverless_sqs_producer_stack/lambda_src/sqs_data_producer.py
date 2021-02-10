import json
import logging
import datetime
import os
import random
import boto3
from botocore.exceptions import ClientError


class GlobalArgs:
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    RELIABLE_QUEUE_NAME = os.getenv("RELIABLE_QUEUE_NAME")


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


def _rand_coin_flip():
    r = False
    if os.getenv("TRIGGER_RANDOM_FAILURES", True):
        if random.randint(1, 100) > 90:
            r = True
    return r


def gen_dob(max_age=99, date_fmt="%Y-%m-%d"):
    return (
        datetime.datetime.today() - datetime.timedelta(days=random.randint(0, 365 * max_age))
    ).strftime(date_fmt)


def get_q_url(sqs_client):
    q = sqs_client.get_queue_url(
        QueueName=GlobalArgs.RELIABLE_QUEUE_NAME).get("QueueUrl")
    LOG.debug(f'{{"q_url":"{q}"}}')
    return q


def send_msg(sqs_client, q_url, msg_body, msg_attr=None):
    if not msg_attr:
        msg_attr = {}
    try:
        LOG.debug(
            f'{{"msg_body":{msg_body}, "msg_attr": {json.dumps(msg_attr)}}}')
        resp = sqs_client.send_message(
            QueueUrl=q_url,
            MessageBody=msg_body,
            MessageAttributes=msg_attr
        )
    except ClientError as e:
        LOG.error(f"ERROR:{str(e)}")
        raise e
    else:
        return resp


LOG = set_logging()
sqs_client = boto3.client("sqs")


def lambda_handler(event, context):
    resp = {"status": False}
    LOG.debug(f"Event: {json.dumps(event)}")

    _random_user_name = ["Aarakocra", "Aasimar", "Beholder", "Bugbear", "Centaur", "Changeling", "Deep Gnome", "Deva", "Lizardfolk", "Loxodon", "Mind Flayer",
                         "Minotaur", "Orc", "Shardmind", "Shifter", "Simic Hybrid", "Tabaxi", "Yuan-Ti"]

    try:
        q_url = get_q_url(sqs_client)
        msg_cnt = 0
        p_cnt = 0
        while context.get_remaining_time_in_millis() > 100:
            _s = round(random.random() * 100, 2)
            msg_body = {
                "name": random.choice(_random_user_name),
                "dob": gen_dob(),
                "gender": random.choice(["M", "F"]),
                "ssn_no": f"{random.randrange(100000000,999999999)}",
                "data_share_consent": bool(random.getrandbits(1)),
                "evnt_time": datetime.datetime.now().isoformat(),
            }
            msg_attr = {
                "project": {
                    "DataType": "String",
                    "StringValue": "Reliable Queues with Dead-Letter-Queue"
                },
                "contact_me": {
                    "DataType": "String",
                    "StringValue": "github.com/miztiik"
                },
                "ts": {
                    "DataType": "Number",
                    "StringValue": f"{int(datetime.datetime.now().timestamp())}"
                },
                "store_id": {
                    "DataType": "Number",
                    "StringValue": f"{random.randrange(1,5)}"
                }
            }
            # Randomly remove ssn_no from message
            if _rand_coin_flip():
                msg_attr.pop("store_id", None)
                msg_attr["bad_msg"] = {
                    "DataType": "String",
                    "StringValue": "True"
                }
                p_cnt += 1
            send_msg(
                sqs_client,
                q_url,
                json.dumps(msg_body),
                msg_attr
            )
            msg_cnt += 1
            LOG.debug(
                f'{{"remaining_time":{context.get_remaining_time_in_millis()}}}')
        resp["tot_msgs"] = msg_cnt
        resp["bad_msgs"] = p_cnt
        resp["status"] = True
        LOG.info(f'{{"resp":{json.dumps(resp)}}}')

    except Exception as e:
        LOG.error(f"ERROR:{str(e)}")
        resp["error_message"] = str(e)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }
