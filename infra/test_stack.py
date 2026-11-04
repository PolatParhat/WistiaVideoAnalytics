"""Checks on the synthesized CloudFormation template (no AWS account needed).

Run:  cd infra && uv run --group infra pytest -q test_stack.py
"""

import json

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template
from wistia_stack import WistiaPipelineStack


@pytest.fixture(scope="module")
def template():
    app = cdk.App()
    stack = WistiaPipelineStack(app, "Test", env=cdk.Environment(region="us-east-1"))
    return Template.from_stack(stack)


def test_bucket_is_private_and_encrypted(template):
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
            "BucketEncryption": Match.any_value(),
        },
    )


def test_schedule_is_created_disabled(template):
    template.has_resource_properties("AWS::Scheduler::Schedule", {"State": "DISABLED"})


def test_lambda_is_python312_arm64(template):
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {"Runtime": "python3.12", "Architectures": ["arm64"], "Handler": "wistia.aws.lambda_handlers.ingest"},
    )


def test_glue_job_uses_iceberg_and_media_config(template):
    jobs = template.find_resources("AWS::Glue::Job")
    args = next(iter(jobs.values()))["Properties"]["DefaultArguments"]
    assert args["--datalake-formats"] == "iceberg"
    assert {m["media_id"] for m in json.loads(args["--media_config"])} == {"8hunphufxp", "9k4tbcdfg0"}
    template.has_resource_properties("AWS::Glue::Job", {"GlueVersion": "5.1", "WorkerType": "G.1X"})


def test_pipeline_order(template):
    machines = template.find_resources("AWS::StepFunctions::StateMachine")
    definition = json.dumps(next(iter(machines.values()))["Properties"]["DefinitionString"])
    assert '\\"StartAt\\":\\"Ingest\\"' in definition
    assert '\\"Ingest\\":{\\"Next\\":\\"Transform\\"' in definition
    assert '\\"Transform\\":{\\"Next\\":\\"AdvanceWatermarks\\"' in definition


def test_failure_alarm_exists(template):
    template.resource_count_is("AWS::CloudWatch::Alarm", 1)
    template.resource_count_is("AWS::Glue::Database", 3)
