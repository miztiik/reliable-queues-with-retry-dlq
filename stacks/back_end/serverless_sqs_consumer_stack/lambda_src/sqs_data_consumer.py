# -*- coding: utf-8 -*-

import json
import logging
import os
import time
import random
import boto3
from botocore.exceptions import ClientError


"""
.. module: sqs_data_consumer
    :Actions: Consume messages from SQS Queue
    :copyright: (c) 2021 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""


__author__ = "Mystique"
__email__ = "miztiik@github"
__version__ = "0.0.1"
__status__ = "production"


class GlobalArgs:
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "sqs_data_consumer"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    RELIABLE_QUEUE_NAME = os.getenv("RELIABLE_QUEUE_NAME")


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    """ Helper to enable logging """
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


LOG = set_logging()
sqs_client = boto3.client("sqs")


def _rand_coin_flip():
    r = False
    if os.getenv("TRIGGER_RANDOM_DELAY", True):
        if random.randint(1, 100) > 90:
            r = True
    return r


def get_q_url(sqs_client):
    q = sqs_client.get_queue_url(
        QueueName=GlobalArgs.RELIABLE_QUEUE_NAME).get("QueueUrl")
    LOG.debug(f'{{"q_url":"{q}"}}')
    return q


def get_msgs(q_url, max_msgs, wait_time):
    try:
        msg_batch = sqs_client.receive_message(
            QueueUrl=q_url,
            MaxNumberOfMessages=max_msgs,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=["All"]
        )
        LOG.debug(f'{{"msg_batch":"{json.dumps(msg_batch)}"}}')
    except ClientError as e:
        LOG.exception(f"ERROR:{str(e)}")
        raise e
    else:
        return msg_batch


def process_msgs(msg_batch):
    try:
        m_process_stat = {}
        err = f'{{"missing_store_id":{True}}}'
        for m in msg_batch:
            # If bad message crash out with exception
            if "messageAttributes" in m and "store_id" not in m.get("messageAttributes"):
                LOG.exception(err)
                raise Exception(err)
            # Randomly time out lambda causing, msg 'visibility Timeout' breach
            # if _rand_coin_flip():
            #     LOG.info(f'{{"trigger_random_delay":{True}}}')
            #     time.sleep(30)
        m_process_stat = {
            "s_msgs": len(msg_batch),
        }
        LOG.debug(f'{{"m_process_stat":"{json.dumps(m_process_stat)}"}}')
    except Exception as e:
        LOG.exception(f"ERROR:{str(e)}")
        raise e
    else:
        return m_process_stat


def del_msgs(q_url, m_to_del):
    sqs_client.delete_message_batch(QueueUrl=q_url, Entries=m_to_del)


def lambda_handler(event, context):
    resp = {"status": False}
    LOG.info(f"Event: {json.dumps(event)}")
    if event["Records"]:
        resp["tot_msgs"] = len(event["Records"])
        LOG.info(f'{{"tot_msgs":{resp["tot_msgs"]}}}')
        m_process_stat = process_msgs(event["Records"])
        resp["s_msgs"] = m_process_stat.get("s_msgs")
        resp["status"] = True
        LOG.info(f'{{"resp":{json.dumps(resp)}}}')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }
