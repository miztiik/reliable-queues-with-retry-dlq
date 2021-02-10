from aws_cdk import aws_iam as _iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as _logs
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


class ServerlessSqsConsumerStack(core.Stack):

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        stack_log_level: str,
        max_msg_receive_cnt: int,
        reliable_queue,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Add your stack resources below)

        # Read Lambda Code
        try:
            with open("stacks/back_end/serverless_sqs_consumer_stack/lambda_src/sqs_data_consumer.py",
                      encoding="utf-8",
                      mode="r"
                      ) as f:
                msg_consumer_fn_code = f.read()
        except OSError:
            print("Unable to read Lambda Function Code")
            raise
        msg_consumer_fn = _lambda.Function(
            self,
            "msgConsumerFn",
            function_name=f"queue_consumer_fn_{construct_id}",
            description="Process messages in SQS queue",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.InlineCode(
                msg_consumer_fn_code),
            handler="index.lambda_handler",
            timeout=core.Duration.seconds(3),
            reserved_concurrent_executions=1,
            environment={
                "LOG_LEVEL": f"{stack_log_level}",
                "APP_ENV": "Production",
                "RELIABLE_QUEUE_NAME": f"{reliable_queue.queue_name}",
                "TRIGGER_RANDOM_DELAY": "True"
            }
        )

        # Create Custom Loggroup for Producer
        msg_consumer_fn_lg = _logs.LogGroup(
            self,
            "msgConsumerFnFnLogGroup",
            log_group_name=f"/aws/lambda/{msg_consumer_fn.function_name}",
            removal_policy=core.RemovalPolicy.DESTROY,
            retention=_logs.RetentionDays.ONE_DAY
        )

        # # Grant our Lambda Consumer privileges to READ from SQS
        # reliable_queue.grant_consume_messages(msg_consumer_fn)

        # Restrict Produce Lambda to be invoked only from the stack owner account
        msg_consumer_fn.add_permission(
            "restrictLambdaInvocationToOwnAccount",
            principal=_iam.AccountRootPrincipal(),
            action="lambda:InvokeFunction",
            source_account=core.Aws.ACCOUNT_ID,
            source_arn=reliable_queue.queue_arn
        )

        # Set our Lambda Function to be invoked by SQS
        msg_consumer_fn.add_event_source(
            _sqsEventSource(reliable_queue, batch_size=5))

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
            "msgConsumer",
            value=f"https://console.aws.amazon.com/lambda/home?region={core.Aws.REGION}#/functions/{msg_consumer_fn.function_name}",
            description="Process messages in SQS queue"
        )
