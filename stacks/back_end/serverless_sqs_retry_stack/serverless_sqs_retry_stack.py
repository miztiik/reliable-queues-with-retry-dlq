from aws_cdk import aws_iam as _iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as _logs
from aws_cdk import aws_sqs as _sqs
from aws_cdk import core
from aws_cdk.aws_lambda_event_sources import SqsEventSource as _sqsEventSource


class GlobalArgs:
    """
    Helper to define global statics
    """

    OWNER = "MystiqueAutomation"
    ENVIRONMENT = "production"
    REPO_NAME = "reliable-queues-with-retry-dlq"
    SOURCE_INFO = f"https://github.com/miztiik/{REPO_NAME}"
    VERSION = "2021_02_07"
    MIZTIIK_SUPPORT_EMAIL = ["mystique@example.com", ]


class ServerlessSqsRetryStack(core.Stack):

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        stack_log_level: str,
        max_msg_receive_cnt: int,
        reliable_queue,
        reliable_queue_dlq,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        # Read Lambda Code
        try:
            with open("stacks/back_end/serverless_sqs_retry_stack/lambda_src/sqs_retry_with_backoff.py",
                      encoding="utf-8",
                      mode="r"
                      ) as f:
                sqs_retry_fn_code = f.read()
        except OSError:
            print("Unable to read Lambda Function Code")
            raise
        sqs_retry_fn = _lambda.Function(
            self,
            "dlqReplayFn",
            function_name=f"sqs_retry_fn_{construct_id}",
            description="Process messages in Retry SQS queue",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.InlineCode(
                sqs_retry_fn_code),
            handler="index.lambda_handler",
            timeout=core.Duration.seconds(3),
            reserved_concurrent_executions=1,
            environment={
                "LOG_LEVEL": f"{stack_log_level}",
                "APP_ENV": "Production",
                "RELIABLE_QUEUE_NAME": f"{reliable_queue.queue_name}",
                "MAX_RECEIVE_CNT": f"{max_msg_receive_cnt}",
                "BACKOFF_RATE": "2",
                "MESSAGE_RETENTION_PERIOD": "172800"
            }
        )

        # Create Custom Loggroup for Producer
        sqs_retry_fn_lg = _logs.LogGroup(
            self,
            "dlqReplayFnLogGroup",
            log_group_name=f"/aws/lambda/{sqs_retry_fn.function_name}",
            removal_policy=core.RemovalPolicy.DESTROY,
            retention=_logs.RetentionDays.ONE_DAY
        )

        # Restrict DLQ Lambda Processor to be invoked only from the stack owner account
        sqs_retry_fn.add_permission(
            "restrictLambdaInvocationToOwnAccount",
            principal=_iam.AccountRootPrincipal(),
            action="lambda:InvokeFunction",
            source_account=core.Aws.ACCOUNT_ID,
            source_arn=reliable_queue_dlq.queue_arn
        )

        # Set our Lambda Function to be invoked by SQS
        sqs_retry_fn.add_event_source(
            _sqsEventSource(reliable_queue_dlq, batch_size=1))

        # Grant our Lambda Producer privileges to write to SQS
        reliable_queue.grant_send_messages(sqs_retry_fn)

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = core.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page."
        )

        output_2 = core.CfnOutput(
            self,
            "dlqReplay",
            value=f"https://console.aws.amazon.com/lambda/home?region={core.Aws.REGION}#/functions/{sqs_retry_fn.function_name}",
            description="Process messages in SQS dead-letter queue"
        )
