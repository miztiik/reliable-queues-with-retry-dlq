#!/usr/bin/env python3

from aws_cdk import core

from reliable_queues_with_retry_dlq.reliable_queues_with_retry_dlq_stack import ReliableQueuesWithRetryDlqStack


app = core.App()
ReliableQueuesWithRetryDlqStack(app, "reliable-queues-with-retry-dlq")

app.synth()
