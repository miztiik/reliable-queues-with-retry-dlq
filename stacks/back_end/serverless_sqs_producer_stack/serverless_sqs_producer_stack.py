from aws_cdk import aws_cloudwatch as _cw
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as _logs
from aws_cdk import aws_sqs as _sqs
from aws_cdk import core


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


class ServerlessSqsProducerStack(core.Stack):

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        stack_log_level: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Add your stack resources below):

        # Maximum number of times, a message can be tried to be process from the queue before deleting
        self.max_msg_receive_cnt = 5
        self.max_msg_receive_cnt_at_retry = 3

        # Define Dead Letter Queue
        self.reliable_q_dlq = _sqs.Queue(
            self,
            "DeadLetterQueue",
            delivery_delay=core.Duration.seconds(100),
            queue_name=f"reliable_q_dlq",
            retention_period=core.Duration.days(2),
            visibility_timeout=core.Duration.seconds(10),
            receive_message_wait_time=core.Duration.seconds(10)
        )

        # Define Retry Queue for Reliable Q
        self.reliable_q_retry_1 = _sqs.Queue(
            self,
            "reliableQueueRetry1",
            delivery_delay=core.Duration.seconds(10),
            queue_name=f"reliable_q_retry_1",
            retention_period=core.Duration.days(2),
            visibility_timeout=core.Duration.seconds(10),
            receive_message_wait_time=core.Duration.seconds(10),
            dead_letter_queue=_sqs.DeadLetterQueue(
                max_receive_count=self.max_msg_receive_cnt_at_retry,
                queue=self.reliable_q_dlq
            )
        )

        # Primary Source Queue
        self.reliable_q = _sqs.Queue(
            self,
            "reliableQueue",
            delivery_delay=core.Duration.seconds(5),
            queue_name=f"reliable_q",
            retention_period=core.Duration.days(2),
            visibility_timeout=core.Duration.seconds(10),
            receive_message_wait_time=core.Duration.seconds(10),
            dead_letter_queue=_sqs.DeadLetterQueue(
                max_receive_count=self.max_msg_receive_cnt,
                queue=self.reliable_q_retry_1
            )
        )

        ########################################
        #######                          #######
        #######     SQS Data Producer    #######
        #######                          #######
        ########################################

        # Read Lambda Code
        try:
            with open("stacks/back_end/serverless_sqs_producer_stack/lambda_src/sqs_data_producer.py",
                      encoding="utf-8",
                      mode="r"
                      ) as f:
                data_producer_fn_code = f.read()
        except OSError:
            print("Unable to read Lambda Function Code")
            raise

        data_producer_fn = _lambda.Function(
            self,
            "sqsDataProducerFn",
            function_name=f"data_producer_fn_{construct_id}",
            description="Produce data events and push to SQS",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.InlineCode(
                data_producer_fn_code),
            handler="index.lambda_handler",
            timeout=core.Duration.seconds(5),
            reserved_concurrent_executions=1,
            environment={
                "LOG_LEVEL": f"{stack_log_level}",
                "APP_ENV": "Production",
                "RELIABLE_QUEUE_NAME": f"{self.reliable_q.queue_name}",
                "TRIGGER_RANDOM_FAILURES": "True"
            }
        )

        # Grant our Lambda Producer privileges to write to SQS
        self.reliable_q.grant_send_messages(data_producer_fn)

        # Create Custom Loggroup for Producer
        data_producer_lg = _logs.LogGroup(
            self,
            "dataProducerLogGroup",
            log_group_name=f"/aws/lambda/{data_producer_fn.function_name}",
            removal_policy=core.RemovalPolicy.DESTROY,
            retention=_logs.RetentionDays.ONE_DAY
        )

        # Restrict Produce Lambda to be invoked only from the stack owner account
        data_producer_fn.add_permission(
            "restrictLambdaInvocationToFhInOwnAccount",
            principal=_iam.AccountRootPrincipal(),
            action="lambda:InvokeFunction",
            source_account=core.Aws.ACCOUNT_ID
        )

        # Monitoring for Queue
        reliable_q_alarm = _cw.Alarm(
            self, "reliableQueueAlarm",
            metric=self.reliable_q.metric(
                "ApproximateNumberOfMessagesVisible"),
            statistic="sum",
            threshold=10,
            period=core.Duration.minutes(5),
            evaluation_periods=1,
            comparison_operator=_cw.ComparisonOperator.GREATER_THAN_THRESHOLD
        )

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = core.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page."
        )

        output_1 = core.CfnOutput(
            self,
            "SqsDataProducer",
            value=f"https://console.aws.amazon.com/lambda/home?region={core.Aws.REGION}#/functions/{data_producer_fn.function_name}",
            description="Produce data events and push to SQS Queue."
        )

        output_2 = core.CfnOutput(
            self,
            "ReliableQueue",
            value=f"https://console.aws.amazon.com/sqs/v2/home?region={core.Aws.REGION}#/queues",
            description="Reliable Queue"
        )

    # properties to share with other stacks
    @property
    def get_queue(self):
        return self.reliable_q

    @property
    def get_dlq(self):
        return self.reliable_q_retry_1
