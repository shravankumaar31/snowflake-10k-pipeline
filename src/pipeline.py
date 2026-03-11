"""Local runner for full Snowflake 10-K pipeline."""

from __future__ import annotations

from src.cloud.lambda_handler import lambda_handler


if __name__ == "__main__":
    lambda_handler({}, object())
