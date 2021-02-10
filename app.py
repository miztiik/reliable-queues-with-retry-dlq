#!/usr/bin/env python3

from stacks.back_end.serverless_sqs_consumer_stack.serverless_sqs_consumer_stack import ServerlessSqsConsumerStack
from stacks.back_end.serverless_sqs_producer_stack.serverless_sqs_producer_stack import ServerlessSqsProducerStack
from stacks.back_end.serverless_sqs_retry_stack.serverless_sqs_retry_stack import ServerlessSqsRetryStack

from aws_cdk import core

app = core.App()


# Produce message events and ingest into SQS queue
sqs_message_producer_stack = ServerlessSqsProducerStack(
    app,
    f"{app.node.try_get_context('project')}-producer-stack",
    stack_log_level="INFO",
    description="Miztiik Automation: Produce message events and ingest into SQS queue"
)

# Consume messages from SQS
reliable_message_queue_stack = ServerlessSqsConsumerStack(
    app,
    f"{app.node.try_get_context('project')}-consumer-stack",
    stack_log_level="INFO",
    reliable_queue=sqs_message_producer_stack.get_queue,
    max_msg_receive_cnt=sqs_message_producer_stack.max_msg_receive_cnt,
    description="Miztiik Automation: Consume messages from SQS"
)

# Replay Messages in DLQ back to main queue with exponential backoff
reliable_message_dlq_replay_stack = ServerlessSqsRetryStack(
    app,
    f"{app.node.try_get_context('project')}-stack",
    stack_log_level="INFO",
    reliable_queue=sqs_message_producer_stack.get_queue,
    reliable_queue_dlq=sqs_message_producer_stack.get_dlq,
    max_msg_receive_cnt=sqs_message_producer_stack.max_msg_receive_cnt,
    description="Miztiik Automation: Replay Messages in DLQ back to main queue with exponential backoff"
)


# Stack Level Tagging
_tags_lst = app.node.try_get_context("tags")

if _tags_lst:
    for _t in _tags_lst:
        for k, v in _t.items():
            core.Tags.of(app).add(k, v, apply_to_launched_instances=True)


app.synth()
