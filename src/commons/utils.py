from datetime import datetime, date
from pytz import timezone
import boto3
import json
from botocore.exceptions import ClientError
from typing import TypeVar
import pandas as pd
import logging
import os
from botocore.config import Config
from urllib.parse import urlparse


DataFrame = TypeVar("DataFrame")

def get_time() -> str:
    """Returns a str of time in the format of {YYYYMMDD}_T{HHMMSS}

    Returns:
        str: A string of time
    """
    tz = timezone("EST")
    now = datetime.now(tz)
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string


def get_date() -> str:
    """Returns a str of date in the format YYYY-MM-DD

    Returns:
        str: A string of date
    """
    date_obj = date.today()
    return date_obj.isoformat()


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


def get_logger(loggername: str, log_level: str):
    """Returns a basic logger with a logger name using a std format

    log level can be set using one of the values in log_levels.
    """
    log_levels = {  # sorted level
        "notset": logging.NOTSET,  # 00
        "debug": logging.DEBUG,  # 10
        "info": logging.INFO,  # 20
        "warning": logging.WARNING,  # 30
        "error": logging.ERROR,  # 40
    }

    logger_filename = loggername + "_" + get_date() + ".log"
    logger = logging.getLogger(loggername)
    logger.setLevel(log_levels[log_level])

    # set the file handler
    file_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    file_handler = logging.FileHandler(logger_filename, mode="w")
    file_handler.setFormatter(logging.Formatter(file_FORMAT, "%H:%M:%S"))
    file_handler.setLevel(log_levels["info"])

    # set the stream handler
    # stream_handler = logging.StreamHandler(sys.stdout)
    # stream_handler.setFormatter(logging.Formatter(file_FORMAT, "%H:%M:%S"))
    # stream_handler.setLevel(log_levels["info"])

    # logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


class ReadSubmTsv:
    """A Class to access tsv file and model associated info"""

    def __init__(self):
        return None

    @staticmethod
    def read_tsv(file_path: str) -> DataFrame:
        """Returns a pandas Dataframe from reading a tsv file

        Args:
            file_path (str): File path of a tsv file

        Returns:
            DataFrame: pandas dataframe
        """        
        tsv_df = pd.read_csv(
            file_path, sep="\t", na_values=["NA", "na", "N/A", "n/a", ""], dtype=str
        )
        return tsv_df

    @staticmethod
    def select_tsv(folder_path: str) -> list[str]:
        """Returns a list of tsv file paths in a folder

        Args:
            folder_path (str): A folder path which contains tsv files

        Returns:
            list[str]: A list of tsv file paths
        """        
        file_list = os.listdir(folder_path)
        tsv_file_list = [
            os.path.join(folder_path, i) for i in file_list if i.endswith(("txt", "tsv"))
        ]
        return tsv_file_list

    @classmethod
    def get_type(cls, file_path: str) -> str:
        """Returns the type value of a tsv file, e.g., study

        Args:
            file_path (str): File path of a tsv file

        Raises:
            ValueError: Raised if more than one type value were found in tsv file

        Returns:
            str: Type value of a tsv file
        """

        tsv_df = cls.read_tsv(file_path=file_path)
        if "type" in tsv_df.columns:
            type_uniq = tsv_df["type"].unique()
        else:
            raise ValueError(f"Can't find type column in the file {file_path}")
        if len(type_uniq) > 1:
            raise ValueError(
                f"More than one type value were found in file {file_path}: {*type_uniq,}"
            )
        else:
            pass
        return type_uniq[0]

    @classmethod
    def select_tsv_exclude_type(cls, folder_path: str, exclude_type_list: list[str]) -> list[str]:
        """
        Args:
            folder_path (str): Folder path of tsv files
            ignore_node_list (list[str]): A list of types that need to be excluded

        Returns:
            list[str]: A list of tsv file paths
        """
        tsv_files = cls.select_tsv(folder_path=folder_path)
        file_keep = []
        for i in tsv_files:
            i_file_type = cls.get_type(file_path=i)
            if i_file_type not in exclude_type_list:
                file_keep.append(i)
            else:
                pass
        return file_keep

    @classmethod
    def select_tsv_include_type(
        cls, folder_path: str, include_type_list: list[str]
    ) -> list[str]:
        """
        Args:
            folder_path (str): Folder path of tsv files
            ignore_node_list (list[str]): A list of types that need to be include

        Returns:
            list[str]: A list of tsv file paths
        """
        tsv_files = cls.select_tsv(folder_path=folder_path)
        file_keep = []
        for i in tsv_files:
            i_file_type = cls.get_type(file_path=i)
            if i_file_type in include_type_list:
                file_keep.append(i)
            else:
                pass
        return file_keep

    @classmethod
    def file_type_mapping(cls, filepath_list: list[str]) -> dict:
        """Returns a dictionary with type as key and file path as value

        Args:
            filepath_list (list[str]): A list of tsv file paths

        Raises:
            ValueError: Raises ValueError if more than more than one files were found with the same type

        Returns:
            dict: A dictioanry with type as key, and filepath as value
        """        
        node_file_mapping = {}
        for i in filepath_list:
            i_type = cls.get_type(file_path=i)
            if i_type in node_file_mapping.keys():
                raise ValueError(
                    f"More than one tsv files for the same node: {i}, {node_file_mapping[i_type]}"
                )
            else:
                node_file_mapping[i_type] = i
        return node_file_mapping

class AwsUtils:
    @staticmethod
    def parse_object_uri(uri: str) -> tuple:
        """Parse object uri into bucket name and key 

        Args:
            uri (str): An s3 uri

        Returns:
            tuple: bucket name and key
        """        
        if not uri.startswith("s3://"):
            uri = "s3://" + uri
        else:
            pass
        parsed_url = urlparse(uri)
        bucket_name = parsed_url.netloc
        object_key = parsed_url.path
        if object_key[0] == "/":
            object_key = object_key[1:]
        else:
            pass
        return bucket_name, object_key

    @staticmethod
    def set_s3_session_client():
        """This method sets the s3 session client object
        to either use localstack for local development if the
        LOCALSTACK_ENDPOINT_URL variable is defined
        """
        localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
        if localstack_endpoint != None:
            AWS_REGION = "us-east-1"
            AWS_PROFILE = "localstack"
            ENDPOINT_URL = localstack_endpoint
            boto3.setup_default_session(profile_name=AWS_PROFILE)
            s3_client = boto3.client(
                "s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL
            )
        else:
            # Create a custom retry configuration
            custom_retry_config = Config(
                connect_timeout=300,
                read_timeout=300,
                retries={
                    "max_attempts": 5,  # Maximum number of retry attempts
                    "mode": "standard",  # Retry on HTTP status codes considered retryable
                },
            )
            s3_client = boto3.client("s3", config=custom_retry_config)
        return s3_client

    @staticmethod
    def set_s3_resource():
        """This method sets the s3_resource object to either use localstack
        for local development if the LOCALSTACK_ENDPOINT_URL variable is
        defined and returns the object
        """
        localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
        if localstack_endpoint != None:
            AWS_REGION = "us-east-1"
            AWS_PROFILE = "localstack"
            ENDPOINT_URL = localstack_endpoint
            boto3.setup_default_session(profile_name=AWS_PROFILE)
            s3_resource = boto3.resource(
                "s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL
            )
        else:
            s3_resource = boto3.resource("s3")
        return s3_resource

    @classmethod
    def file_dl(cls, bucket: str, filepath: str) -> str:
        """Downloads an object/file from AWS bucket

        Args:
            bucket (str): Bucket name
            filepath (str): File path in the bucket

        Raises:
            ClientError: Raised if file can't be downloaded
            
        Returns:
            str: File name of downloaded file
        """
        # Set the s3 resource object for local or remote execution
        s3 = cls.set_s3_resource()
        bucket_resource = s3.Bucket(bucket)
        filename = os.path.basename(filepath)
        try:
            bucket_resource.download_file(filepath, filename)
        except ClientError as ex:
            ex_code = ex.response["Error"]["Code"]
            ex_message = ex.response["Error"]["Message"]
            raise ClientError(f"Error occurred while downloading file {filename} from bucket {bucket}:\n{ex_code}, {ex_message}")
        return filename

    @classmethod
    def file_ul(cls, bucket: str, output_folder: str,  newfile: str, subfolder: str = "") -> None:
        """Uploads an object from local to AWS bucket

        Args:
            bucket (str): AWS bucket name
            output_folder (str): Folder path in dest bucket
            newfile (str): Filename in local
            subfolder (str, optional): Defaults to "".
        """
        s3 = cls.set_s3_resource()
        bucket_resource = s3.Bucket(bucket)
        # upload files outside inputs/ folder
        file_key = os.path.join(output_folder, subfolder, newfile)
        try:
            bucket_resource.upload_file(newfile, file_key)
        except ClientError as ex:
            ex_code = ex.response["Error"]["Code"]
            ex_message = ex.response["Error"]["Message"]
            raise ClientError(
                f"Error occurred while uploading file {newfile} to bucket {bucket}:\n{ex_code}, {ex_message}"
            )
        return None

    @classmethod
    def folder_dl(cls, bucket: str, remote_folder_path: str) -> str:
        """Downloads a folder from AWS bucket. The downloaded folder follows 
        same structure as input, remote_folder_path

        Args:
            bucket (str): AWS bucket name
            remote_folder_path (str): folder path in remote bucket

        Returns:
            str: folder path of downloaded folder, equals to remote_folder_path
        """
        s3 = cls.set_s3_resource()
        bucket_resource = s3.Bucket(bucket)
        for obj in bucket_resource.objects.filter(Prefix=remote_folder_path):
            if not os.path.exists(os.path.dirname(obj.key)):
                os.makedirs(os.path.dirname(obj.key))
            try:
                bucket_resource.download_file(obj.key, obj.key)
            except NotADirectoryError as err:
                err_str = repr(err)
                print(
                    f"Error downloading folder {remote_folder_path} from bucket {bucket}: {err_str}"
                )
        return remote_folder_path

    @classmethod
    def folder_ul(cls, local_folder: str, bucket: str, destination: str, sub_folder: str = "") -> str:
        """Uploads a folder from local to AWS bucket
        

        Args:
            local_folder (str): local folder path
            bucket (str): AWS bucket name
            destination (str): remote AWS path for folder
            sub_folder (str, optional): Any subfolder under destination path if applicable. Defaults to "".

        Returns:
            str: copies folder path in remote AWS bucket
        """
        s3 = cls.set_s3_resource()
        bucket_resource = s3.Bucket(bucket)
        folder_basename = os.path.basename(local_folder)
        for root, _, files in os.walk(local_folder):
            for filename in files:
                # construct local path
                local_path = os.path.join(root, filename)

                # construct the full dst path
                relative_path = os.path.relpath(local_path, local_folder)
                s3_path = os.path.join(
                    destination, sub_folder, folder_basename, relative_path
                )

                # upload file
                # this should overwrite file if file exists in the bucket
                bucket_resource.upload_file(local_path, s3_path)
        remote_folder_path = os.path.join(destination, sub_folder, folder_basename)
        return remote_folder_path
