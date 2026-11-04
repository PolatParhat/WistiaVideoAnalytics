"""Builds the Lambda code package on this machine (no Docker needed).

The package = the `wistia` library + `requests` (boto3 is already in the Lambda runtime).
uv downloads the wheels for Lambda's platform (Linux, arm64, Python 3.12), whatever machine runs `cdk synth`.
"""

import shutil
import subprocess
from pathlib import Path

import jsii
from aws_cdk import BundlingOptions, DockerImage, ILocalBundling

REPO_ROOT = Path(__file__).resolve().parents[1]
LAMBDA_REQUIREMENTS = ["requests>=2.32"]


@jsii.implements(ILocalBundling)
class LocalLambdaBundler:
    def try_bundle(self, output_dir, *, image=None, **kwargs):
        subprocess.run(
            [
                "uv", "pip", "install", "--quiet",
                "--target", output_dir,
                "--python-platform", "aarch64-manylinux2014",
                "--python-version", "3.12",
                "--only-binary", ":all:",
                *LAMBDA_REQUIREMENTS,
            ],
            check=True,
        )  # fmt: skip
        shutil.copytree(
            REPO_ROOT / "src" / "wistia",
            Path(output_dir) / "wistia",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        return True


def lambda_bundling():
    # `image` is only used if local bundling fails; local bundling always succeeds where uv is installed.
    return BundlingOptions(
        image=DockerImage.from_registry("public.ecr.aws/sam/build-python3.12"), local=LocalLambdaBundler()
    )
