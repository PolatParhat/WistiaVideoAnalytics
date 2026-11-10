"""One-time stack: lets the GitHub deploy workflow log in to AWS without stored keys (OIDC).

Deployed once by hand, before the first GitHub deploy:
  cdk deploy WistiaGithubOidc
Then copy the DeployRoleArn output into the GitHub secret AWS_DEPLOY_ROLE_ARN.

The role can only be used by this repository's `production` environment, and it can only do one thing:
assume the CDK bootstrap roles (that is how `cdk deploy` works).
"""

from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_iam as iam
from constructs import Construct

GITHUB_REPO = "PolatParhat/WistiaVideoAnalytics"


class GithubOidcStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, create_provider: bool = True, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        url = "token.actions.githubusercontent.com"
        if create_provider:
            provider = iam.OpenIdConnectProvider(
                self, "GithubProvider", url=f"https://{url}", client_ids=["sts.amazonaws.com"]
            )
            provider_arn = provider.open_id_connect_provider_arn
        else:  # the account already has one (only one per URL is allowed)
            provider_arn = f"arn:aws:iam::{self.account}:oidc-provider/{url}"

        role = iam.Role(
            self,
            "DeployRole",
            role_name="wistia-github-deploy",
            assumed_by=iam.WebIdentityPrincipal(
                provider_arn,
                conditions={
                    "StringEquals": {
                        f"{url}:aud": "sts.amazonaws.com",
                        f"{url}:sub": f"repo:{GITHUB_REPO}:environment:production",
                    }
                },
            ),
            description="GitHub Actions deploy role for the Wistia pipeline (cdk deploy)",
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["sts:AssumeRole"],
                resources=[f"arn:aws:iam::{self.account}:role/cdk-hnb659fds-*-{self.account}-{self.region}"],
            )
        )
        CfnOutput(self, "DeployRoleArn", value=role.role_arn)
