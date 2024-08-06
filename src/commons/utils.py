from datetime import datetime
from pytz import timezone
import boto3
import json
from botocore.exceptions import ClientError


def get_time() -> str:
    """Returns the current time"""
    tz = timezone("EST")
    now = datetime.now(tz)
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string


def get_secret(secret_name: str, region_name: str="us-east-1") -> dict:
    """Returns a dictionary that contains all secret values of secret name

    Args:
        secret_name (str): A secret name
        region_name (str, optional): Defaults to "us-east-1".

    Raises:
        e: Raise ClientError

    Returns:
        dict: Dictionary of secret_name value
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
