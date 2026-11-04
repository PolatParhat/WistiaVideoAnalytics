"""CDK app with two stacks.

WistiaPipeline    the whole pipeline (deployed by you or by the GitHub Deploy workflow)
WistiaGithubOidc  one-time: the role GitHub Actions uses to deploy (no stored AWS keys)

  cd infra
  cdk synth                                                   # build the templates (no AWS calls)
  cdk deploy WistiaPipeline -c alert_email=you@example.com    # create/update the pipeline
  cdk destroy WistiaPipeline                                  # delete it again
"""

import aws_cdk as cdk
from github_oidc_stack import GithubOidcStack
from wistia_stack import WistiaPipelineStack

app = cdk.App()
env = cdk.Environment(region="us-east-1")

WistiaPipelineStack(
    app,
    "WistiaPipeline",
    alert_email=app.node.try_get_context("alert_email"),
    env=env,
    description="Wistia video analytics pipeline: Lambda ingest -> S3 raw -> Glue PySpark -> Iceberg -> Athena",
)
GithubOidcStack(
    app,
    "WistiaGithubOidc",
    # -c create_oidc_provider=false if the account already has a GitHub OIDC provider
    create_provider=app.node.try_get_context("create_oidc_provider") != "false",
    env=env,
    description="GitHub Actions OIDC deploy role for the Wistia pipeline",
)
cdk.Tags.of(app).add("project", "wistia-video-analytics")
app.synth()
