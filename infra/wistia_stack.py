"""The whole pipeline as one CDK stack (see docs/02_design/README.md, section 1)."""

import sys
from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from lambda_bundle import lambda_bundling

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
from wistia.config import media_config_json  # noqa: E402  (reads config/media.yaml at synth time)

SECRET_NAME = "wistia/api-token"  # created once by the user with the AWS CLI (see README)
GLUE_DATABASES = ["wistia_silver", "wistia_gold", "wistia_ops"]
ICEBERG_CONF = [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.warehouse={warehouse}",
    "spark.sql.catalog.lake.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.session.timeZone=UTC",
]


class WistiaPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, alert_email: str | None = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        media_config = media_config_json()

        # ------------------------------------------------------------ storage
        # One private, encrypted bucket: raw/ (API responses), warehouse/ (Iceberg tables), athena-results/.
        # DESTROY + auto-delete so `cdk destroy` leaves nothing behind (learning project; see decisions.md).
        bucket = s3.Bucket(
            self,
            "DataBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[s3.LifecycleRule(prefix="athena-results/", expiration=Duration.days(7))],
        )
        warehouse = f"s3://{bucket.bucket_name}/warehouse"

        watermarks = dynamodb.TableV2(
            self,
            "Watermarks",
            partition_key=dynamodb.Attribute(name="media_id", type=dynamodb.AttributeType.STRING),
            billing=dynamodb.Billing.on_demand(),
            removal_policy=RemovalPolicy.DESTROY,
        )

        secret = secretsmanager.Secret.from_secret_name_v2(self, "WistiaToken", SECRET_NAME)

        # ------------------------------------------------------------ ingestion (Lambda)
        code = lambda_.Code.from_asset(str(REPO_ROOT / "src"), bundling=lambda_bundling())
        env = {
            "RAW_BUCKET": bucket.bucket_name,
            "WATERMARK_TABLE": watermarks.table_name,
            "WISTIA_SECRET_ID": SECRET_NAME,
            "MEDIA_CONFIG": media_config,
        }

        def function(name, handler, timeout):
            log_group = logs.LogGroup(
                self, f"{name}Logs", retention=logs.RetentionDays.ONE_MONTH, removal_policy=RemovalPolicy.DESTROY
            )
            return lambda_.Function(
                self,
                name,
                runtime=lambda_.Runtime.PYTHON_3_12,
                architecture=lambda_.Architecture.ARM_64,
                handler=f"wistia.aws.lambda_handlers.{handler}",
                code=code,
                timeout=timeout,
                memory_size=256,
                environment=env,
                log_group=log_group,
            )

        ingest_fn = function("IngestFunction", "ingest", Duration.minutes(5))
        advance_fn = function("AdvanceWatermarksFunction", "advance_watermarks", Duration.seconds(30))
        bucket.grant_put(ingest_fn, "raw/*")
        watermarks.grant_read_data(ingest_fn)
        watermarks.grant_read_write_data(advance_fn)
        secret.grant_read(ingest_fn)

        # ------------------------------------------------------------ transformation (Glue + Iceberg)
        for db in GLUE_DATABASES:
            glue.CfnDatabase(
                self,
                f"Db{db}",
                catalog_id=self.account,
                database_input=glue.CfnDatabase.DatabaseInputProperty(name=db, location_uri=f"{warehouse}/{db}.db"),
            )

        script = s3_assets.Asset(self, "GlueScript", path=str(REPO_ROOT / "jobs" / "glue_transform.py"))
        library = s3_assets.Asset(  # a folder asset is uploaded as a .zip: Glue adds it to the Python path
            self, "GlueLibrary", path=str(REPO_ROOT / "src"), exclude=["**/__pycache__", "**/*.pyc", "*.egg-info"]
        )

        glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )
        bucket.grant_read(glue_role, "raw/*")
        bucket.grant_read_write(glue_role, "warehouse/*")
        script.grant_read(glue_role)
        library.grant_read(glue_role)

        glue_job = glue.CfnJob(
            self,
            "TransformJob",
            name="wistia-transform",
            role=glue_role.role_arn,
            glue_version="5.1",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=30,  # minutes
            max_retries=0,  # Step Functions does the retries
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl", python_version="3", script_location=script.s3_object_url
            ),
            default_arguments={
                "--job-language": "python",
                "--extra-py-files": library.s3_object_url,
                "--datalake-formats": "iceberg",
                "--conf": " --conf ".join(c.format(warehouse=warehouse) for c in ICEBERG_CONF),
                "--raw_root": f"s3://{bucket.bucket_name}",
                "--media_config": media_config,
                "--run_date": "set-by-step-functions",
                "--run_id": "set-by-step-functions",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
            },
        )

        # ------------------------------------------------------------ orchestration (Step Functions)
        retry = {"errors": ["States.ALL"], "max_attempts": 2, "interval": Duration.seconds(30), "backoff_rate": 2}

        ingest = tasks.LambdaInvoke(
            self, "Ingest", lambda_function=ingest_fn, payload_response_only=True, result_path="$.ingest"
        )
        ingest.add_retry(**retry)

        transform = tasks.GlueStartJobRun(
            self,
            "Transform",
            glue_job_name=glue_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # wait for the job to finish
            arguments=sfn.TaskInput.from_object(
                {
                    "--run_date": sfn.JsonPath.string_at("$.ingest.run_date"),
                    "--run_id": sfn.JsonPath.string_at("$.ingest.run_id"),
                }
            ),
            result_path="$.transform",
        )
        transform.add_retry(**retry)

        advance = tasks.LambdaInvoke(
            self,
            "AdvanceWatermarks",
            lambda_function=advance_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "run_id": sfn.JsonPath.string_at("$.ingest.run_id"),
                    "run_date": sfn.JsonPath.string_at("$.ingest.run_date"),
                    "media_ids": sfn.JsonPath.list_at("$.ingest.media_ids"),
                }
            ),
            payload_response_only=True,
            result_path="$.watermarks",
        )
        advance.add_retry(**retry)

        definition = ingest.next(transform).next(advance).next(sfn.Succeed(self, "Done"))
        state_machine = sfn.StateMachine(
            self,
            "Pipeline",
            state_machine_name="wistia-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(1),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self, "PipelineLogs", retention=logs.RetentionDays.ONE_MONTH, removal_policy=RemovalPolicy.DESTROY
                ),
                level=sfn.LogLevel.ERROR,
            ),
        )

        # ------------------------------------------------------------ schedule (created DISABLED)
        scheduler_role = iam.Role(self, "SchedulerRole", assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"))
        state_machine.grant_start_execution(scheduler_role)
        scheduler.CfnSchedule(
            self,
            "DailySchedule",
            name="wistia-daily",
            description="Daily run at 02:00 UTC for yesterday. Disabled: FR8 is met by the simulated week.",
            state="DISABLED",
            schedule_expression="cron(0 2 * * ? *)",
            schedule_expression_timezone="UTC",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(mode="OFF"),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input='{"scheduled_time": "<aws.scheduler.scheduled-time>"}',
            ),
        )

        # ------------------------------------------------------------ monitoring
        topic = sns.Topic(self, "Alerts", display_name="Wistia pipeline alerts")
        if alert_email:
            topic.add_subscription(subs.EmailSubscription(alert_email))
        alarm = cloudwatch.Alarm(
            self,
            "PipelineFailed",
            alarm_description="A Wistia pipeline run failed or timed out",
            metric=state_machine.metric_failed(period=Duration.minutes(5), statistic="Sum").with_(
                period=Duration.minutes(5)
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        alarm.add_alarm_action(cw_actions.SnsAction(topic))

        # ------------------------------------------------------------ query (Athena)
        athena.CfnWorkGroup(
            self,
            "Workgroup",
            name="wistia",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3"
                ),
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{bucket.bucket_name}/athena-results/"
                ),
            ),
        )

        # ------------------------------------------------------------ outputs (printed after deploy)
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        CfnOutput(self, "WatermarkTable", value=watermarks.table_name)
        CfnOutput(self, "GlueJobName", value=glue_job.name)
        CfnOutput(self, "AthenaWorkgroup", value="wistia")
