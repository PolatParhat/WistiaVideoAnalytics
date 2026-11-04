"""CDK app: one stack with the whole pipeline.

cd infra
cdk synth                                   # build the CloudFormation template (no AWS calls)
cdk deploy -c alert_email=you@example.com   # create/update everything in your AWS account
cdk destroy                                 # delete everything again
"""

import aws_cdk as cdk
from wistia_stack import WistiaPipelineStack

app = cdk.App()
WistiaPipelineStack(
    app,
    "WistiaPipeline",
    alert_email=app.node.try_get_context("alert_email"),
    env=cdk.Environment(region="us-east-1"),
    description="Wistia video analytics pipeline: Lambda ingest -> S3 raw -> Glue PySpark -> Iceberg -> Athena",
)
cdk.Tags.of(app).add("project", "wistia-video-analytics")
app.synth()
