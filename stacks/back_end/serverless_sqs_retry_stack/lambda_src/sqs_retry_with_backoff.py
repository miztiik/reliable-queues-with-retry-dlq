import datetime
import json
import logging
import os
import random
import uuid
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


class GlobalArgs:
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "sqs_data_consumer"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    RELIABLE_QUEUE_NAME = os.getenv("RELIABLE_QUEUE_NAME")
    MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", 3))
    BACKOFF_RATE = int(os.getenv("BACKOFF_RATE", 2))
    MESSAGE_RETENTION_PERIOD = int(os.getenv("MESSAGE_RETENTION_PERIOD"))


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


LOG = set_logging()
sqs_client = boto3.client("sqs")


def get_q_url(sqs_client):
    q = sqs_client.get_queue_url(
        QueueName=GlobalArgs.RELIABLE_QUEUE_NAME).get("QueueUrl")
    LOG.debug(f'{{"q_url":"{q}"}}')
    return q


def del_msgs(q_url, m_to_del):
    sqs_client.delete_message_batch(QueueUrl=q_url, Entries=m_to_del)
    LOG.info(f'{{"m_del_status":True}}')


def lambda_handler(event, context):
    resp = {"status": False}
    LOG.debug(f"Event: {json.dumps(event)}")
    q_url = get_q_url(sqs_client)
    resp["tot_msgs"] = len(event["Records"])
    for record in event["Records"]:
        replay_cnt = 0
        if "sqs-dlq-replay-cnt" in record['messageAttributes']:
            replay_cnt = int(record['messageAttributes']
                             ["sqs-dlq-replay-cnt"]["stringValue"])
        LOG.info(f'{{"replay_cnt":{replay_cnt}}}')
        replay_cnt += 1
        if replay_cnt > GlobalArgs.MAX_ATTEMPTS:
            raise MaxAttemptsError(
                replay=replay_cnt, max=GlobalArgs.MAX_ATTEMPTS)
        attributes = record['messageAttributes']
        attributes.update(
            {"sqs-dlq-replay-cnt": {'StringValue': str(replay_cnt), 'DataType': 'Number'}})

        LOG.debug(f'{{"dirty_sqs_msg_attributes":{attributes}}}')
        _sqs_attrib_cleaner(attributes)
        LOG.debug(f'{{"clean_sqs_msg_attributes":{attributes}}}')

        # Backoff
        b = ExpoBackoffFullJitter(
            base=GlobalArgs.BACKOFF_RATE,
            cap=GlobalArgs.MESSAGE_RETENTION_PERIOD)
        delaySeconds = b.Backoff(n=int(replay_cnt))

        resp["replay_cnt"] = replay_cnt
        resp["max_attempts"] = GlobalArgs.MAX_ATTEMPTS
        resp["replayed_to_main_q"] = True
        resp["delay_sec"] = delaySeconds

        sqs_client.send_message(
            QueueUrl=q_url,
            MessageBody=record["body"],
            DelaySeconds=int(delaySeconds),
            MessageAttributes=record["messageAttributes"]
        )
        resp["status"] = True
        LOG.info(f'{{"resp":{json.dumps(resp)}}}')


def _sqs_attrib_cleaner(attributes):
    d = dict.fromkeys(attributes)
    for k in d:
        if isinstance(attributes[k], dict):
            subd = dict.fromkeys(attributes[k])
            for subk in subd:
                if not attributes[k][subk]:
                    del attributes[k][subk]
                else:
                    attributes[k][''.join(
                        subk[:1].upper() + subk[1:])] = attributes[k].pop(subk)


class MaxAttemptsError(Exception):
    def __init__(self, replay, max, msg=None):
        if msg is None:
            msg = "Number of retries(%s) > max attempts(%s)" % (replay, max)
        super(MaxAttemptsError, self).__init__(msg)
        self.max = max
        self.replay = replay


class Backoff:

    def __init__(self, base, cap):
        """Init."""
        self.base = base
        self.cap = cap

    def expo(self, n):
        """Backoff function."""
        return min(self.cap, pow(2, n) * self.base)


class ExpoBackoffFullJitter(Backoff):

    def Backoff(self, n):
        """Full jitter Backoff function."""
        base = self.expo(n)
        fulljitter = random.uniform(0, base)
        # print("Backoff: %s - Full Jitter Backoff: %s" % (base, fulljitter))
        return fulljitter
