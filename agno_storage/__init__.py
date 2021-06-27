"""
cloud agnostic storage service class which gives methods to integrate your file storage to multiple cloud support
How to use eg:
    1.display all boxes your storage contains i.e (buckets in S3/containers in Azure ADLS)
        use obj.get_all_boxes() # will return list of boxes i.e (buckets in S3/containers in Azure ADLS)
    2.create box in your storage
        use obj.create_box() # will create box in your storage i.e (buckets in S3/containers in Azure ADLS)
Add keys to your settings.py

AGNOSTIC_STORAGE_CLOUD_PLATFORM : any of ["AWS", "AZURE",]

Add cloud specific keys to your settings.py

FOR AWS:
    AGNOSTIC_STORAGE_AWS_ACCESS_KEY = "****" (aws s3 access key)
    AGNOSTIC_STORAGE_AWS_SECRET_KEY = "****" (aws s3 secret key)
    AGNOSTIC_STORAGE_AWS_REGION = "****" (aws s3 region)
FOR AZURE:
    AGNOSTIC_STORAGE_AZURE_ACCOUNT_NAME = "****" (azure storage account name)
    AGNOSTIC_STORAGE_AZURE_ACCESS_KEY = "****" (azure storage access key)
    AGNOSTIC_STORAGE_AZURE_CONNECTION_STR = "****" (azure storage connection string)
As of right now this library is supporting following cloud Storage:
    1. AWS S3
    2. Azure ADLS
Note : Kindly fork and contribute for future release!!!
"""
from datetime import datetime, timedelta

import boto3
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.storage.filedatalake import DataLakeServiceClient
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from django.conf import settings

CHOICES = ["AWS", "AZURE"]

try:
    AGNOSTIC_STORAGE_CLOUD_PLATFORM = settings.AGNOSTIC_STORAGE_CLOUD_PLATFORM
except AttributeError:
    raise AttributeError(
        f"set AGNOSTIC_STORAGE_CLOUD_PLATFORM key in your settings.py to either str : {', '.join(CHOICES)}")
if AGNOSTIC_STORAGE_CLOUD_PLATFORM not in CHOICES:
    raise ValueError("AGNOSTIC_STORAGE_CLOUD_PLATFORM must any of str : " + ", ".join(CHOICES))

if AGNOSTIC_STORAGE_CLOUD_PLATFORM == CHOICES[0]:
    AGNOSTIC_STORAGE_AWS_ACCESS_KEY = settings.AGNOSTIC_STORAGE_AWS_ACCESS_KEY
    AGNOSTIC_STORAGE_AWS_SECRET_KEY = settings.AGNOSTIC_STORAGE_AWS_SECRET_KEY
    AGNOSTIC_STORAGE_AWS_REGION = settings.AGNOSTIC_STORAGE_AWS_REGION
    if not (AGNOSTIC_STORAGE_AWS_ACCESS_KEY
            and AGNOSTIC_STORAGE_AWS_SECRET_KEY
            and AGNOSTIC_STORAGE_AWS_REGION):
        raise ValueError("cloud agnostic storage AWS keys settings can't be empty")
elif AGNOSTIC_STORAGE_CLOUD_PLATFORM == CHOICES[1]:
    AGNOSTIC_STORAGE_AZURE_ACCOUNT_NAME = settings.AGNOSTIC_STORAGE_AZURE_ACCOUNT_NAME
    AGNOSTIC_STORAGE_AZURE_ACCESS_KEY = settings.AGNOSTIC_STORAGE_AZURE_ACCESS_KEY
    AGNOSTIC_STORAGE_AZURE_CONNECTION_STR = settings.AGNOSTIC_STORAGE_AZURE_CONNECTION_STR
    if not (AGNOSTIC_STORAGE_AZURE_ACCOUNT_NAME
            and AGNOSTIC_STORAGE_AZURE_ACCESS_KEY
            and AGNOSTIC_STORAGE_AZURE_CONNECTION_STR):
        raise ValueError("cloud agnostic storage AZURE keys settings can't be empty")

PRESIGNED_URL_METHODS = {
    "READ": "get_object",
    "WRITE": "put_object"
}

# size is in GB
S3_MULTIPART_SIZE = 1024 ** 3
# any file more than 1GB will be broken into multi parts and send concurrently in chunks
S3_LIST_CONFIG = TransferConfig(multipart_threshold=S3_MULTIPART_SIZE, multipart_chunksize=S3_MULTIPART_SIZE)


class _AdlsStorage:
    __conn_str = None
    __adls_client = None
    __blob_client = None
    __acc_name = None
    __acc_key = None

    def __init__(self, acc_name, acc_key, conn_str):
        self.__conn_str = conn_str
        self.__adls_client = DataLakeServiceClient(account_url=f"https://{acc_name}.dfs.core.windows.net",
                                                   credential=acc_key)
        self.__blob_client = BlobServiceClient.from_connection_string(conn_str)
        self.__acc_name = acc_name
        self.__acc_key = acc_key

    def list_containers(self):
        return [container.name for container in self.__blob_client.list_containers()]

    def create_container(self, container_name):
        self.__adls_client.create_file_system(file_system=container_name)

    def list_blobs_in_container(self, container_name):
        _list = self.__adls_client.get_file_system_client(file_system=container_name).get_paths()
        return [blob.name for blob in _list]

    def upload_file(self, container_name, file_path, key):
        file_system_client = self.__adls_client.get_file_system_client(file_system=container_name)
        directory_client = file_system_client.get_directory_client("/")
        file_client = directory_client.create_file(key)
        with open(file_path, 'r') as local_file:
            file_contents = local_file.read()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))

    def upload_big_file(self, container_name, file_path, key):
        file_system_client = self.__adls_client.get_file_system_client(file_system=container_name)
        directory_client = file_system_client.get_directory_client("/")
        file_client = directory_client.get_file_client(key)
        with open(file_path, 'r') as local_file:
            file_contents = local_file.read()
        file_client.upload_data(file_contents, overwrite=True)

    def download_file(self, container_name, key):
        file_system_client = self.__adls_client.get_file_system_client(file_system=container_name)
        directory_client = file_system_client.get_directory_client("/")
        file_client = directory_client.get_file_client(key)
        return file_client.download_file()

    def get_blob_sas(self, container_name: str, key: str, action: str, expire_in_sec: int) -> str:
        expire_in_hours = int(expire_in_sec / 60 / 60)
        _read = False
        _write = False
        if action == "READ":
            _read = True
        elif action == "WRITE":
            _write = True
        sas_blob = generate_blob_sas(account_name=self.__acc_name,
                                     container_name=container_name,
                                     blob_name=key,
                                     account_key=self.__acc_key,
                                     permission=BlobSasPermissions(read=_read, write=_write),
                                     expiry=datetime.utcnow() + timedelta(hours=expire_in_hours))
        return f"https://{self.__acc_name}.blob.core.windows.net/{container_name}/{key}?{sas_blob}"

    def check_if_file_exists(self, container_name: str, key: str) -> bool:
        file_system_client = self.__adls_client.get_file_system_client(file_system=container_name)
        if file_system_client.get_file_client(key).exists():
            return True
        return False

    def put_object(self, container_name, key):
        pass


class _S3Storage:
    __client = None
    __region_name = None

    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.__region_name = region_name
        self.__client = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key,
                                     region_name=region_name)

    def list_buckets(self):
        return [bucket["Name"] for bucket in self.__client.list_buckets()["Buckets"]]

    def create_bucket(self, bucket_name):
        self.__client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration={'LocationConstraint': self.__region_name})

    def get_object(self, bucket_name, key):
        return self.__client.get_object(Bucket=bucket_name, Key=key).get('Body')

    def list_files_in_bucket(self, bucket_name):
        try:
            return [file_name for file_name in self.__client.list_objects(Bucket=bucket_name)['Contents']]
        except KeyError:
            return []

    def upload_file(self, file_path, bucket_name, key):
        self.__client.upload_file(file_path, bucket_name, key)

    def upload_big_csv_with_configs(self, file_path, bucket_name, key, configs):
        self.__client.upload_file(file_path, bucket_name, key, ExtraArgs={'ContentType': 'text/csv'}, Config=configs)

    def generate_presigned_url(self, bucket_name, key_path, action, expires_in):
        return self.__client.generate_presigned_url(PRESIGNED_URL_METHODS[action],
                                                    Params={'Bucket': bucket_name, 'Key': key_path},
                                                    ExpiresIn=expires_in)

    def get_files_from_directory(self, bucket_name, key):
        try:
            return self.__client.list_objects_v2(Bucket=bucket_name, Prefix=key, Delimiter='/').get('Contents', [])
        except KeyError:
            return []

    def check_if_file_exists(self, bucket_name, key):
        try:
            self.__client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def put_object(self, bucket_name, key_path, configs):
        self.__client.put_object(Bucket=bucket_name, Body=configs, Key=key_path)


class StorageService:
    """
    cloud agnostic storage service class which gives methods to integrate your file storage to multiple cloud support
    How to use eg:
        1.display all boxes your storage contains i.e (buckets in S3/containers in Azure ADLS)
            use obj.get_all_boxes() # will return list of boxes i.e (buckets in S3/containers in Azure ADLS)
        2.create box in your storage
            use obj.create_box() # will create box in your storage i.e (buckets in S3/containers in Azure ADLS)
    Add keys to your settings.py

    AGNOSTIC_STORAGE_CLOUD_PLATFORM : any of ["AWS", "AZURE",]

    Add cloud specific keys to your settings.py

    FOR AWS:
        AGNOSTIC_STORAGE_AWS_ACCESS_KEY = "****" (aws s3 access key)
        AGNOSTIC_STORAGE_AWS_SECRET_KEY = "****" (aws s3 secret key)
        AGNOSTIC_STORAGE_AWS_REGION = "****" (aws s3 region)
    FOR AZURE:
        AGNOSTIC_STORAGE_AZURE_ACCOUNT_NAME = "****" (azure storage account name)
        AGNOSTIC_STORAGE_AZURE_ACCESS_KEY = "****" (azure storage access key)
        AGNOSTIC_STORAGE_AZURE_CONNECTION_STR = "****" (azure storage connection string)
    """
    __s3_obj = None
    __adls_obj = None

    def __init__(self):
        if AGNOSTIC_STORAGE_CLOUD_PLATFORM not in CHOICES:
            raise ValueError("cloud_platform must any of : " + ",".join(CHOICES))
        elif AGNOSTIC_STORAGE_CLOUD_PLATFORM == "AWS":
            self.__s3_obj = _S3Storage(AGNOSTIC_STORAGE_AWS_ACCESS_KEY,
                                       AGNOSTIC_STORAGE_AWS_SECRET_KEY,
                                       AGNOSTIC_STORAGE_AWS_REGION)
        elif AGNOSTIC_STORAGE_CLOUD_PLATFORM == "AZURE":
            self.__adls_obj = _AdlsStorage(AGNOSTIC_STORAGE_AZURE_ACCOUNT_NAME,
                                           AGNOSTIC_STORAGE_AZURE_ACCESS_KEY,
                                           AGNOSTIC_STORAGE_AZURE_CONNECTION_STR)
        else:
            # for future release (support for more clouds)
            pass

    def get_all_boxes(self) -> list:
        """
        method to return all boxes in storage
        :return: list
        """
        if self.__s3_obj:
            return self.__s3_obj.list_buckets()
        elif self.__adls_obj:
            return self.__adls_obj.list_containers()

    def create_box(self, box_name: str) -> None:
        """
        method to create box in storage
        :param box_name: unique throughout cloud
        :return: None
        """
        if self.__s3_obj:
            return self.__s3_obj.create_bucket(bucket_name=box_name)
        elif self.__adls_obj:
            return self.__adls_obj.create_container(container_name=box_name)

    def get_all_records_in_box(self, box_name: str) -> list:
        """
        method to get all records in box
        :param box_name: str
        :return: list
        """
        if self.__s3_obj:
            return self.__s3_obj.list_files_in_bucket(bucket_name=box_name)
        elif self.__adls_obj:
            return self.__adls_obj.list_blobs_in_container(container_name=box_name)

    def post_record_to_box(self, file_path: str, box_name: str, key: str) -> None:
        """
        method to upload record to box
        :param file_path: local file path str
        :param box_name: box name str
        :param key: file name str
        :return: None
        """
        if self.__s3_obj:
            self.__s3_obj.upload_file(file_path, box_name, key)
        elif self.__adls_obj:
            self.__adls_obj.upload_file(box_name, file_path, key)

    def post_big_csv_record_to_box_with_configs(self, file_path: str, box_name: str,
                                                key: str, configs: TransferConfig = S3_LIST_CONFIG) -> None:
        """
        method to create big csv record with configs of concurrency
        :param file_path: local file path str
        :param box_name: box name str
        :param key: file name str
        :param configs: TransferConfig (default=1gb in concurrency)
        :return: None
        """
        if self.__s3_obj:
            self.__s3_obj.upload_big_csv_with_configs(file_path, box_name, key, configs)
        elif self.__adls_obj:
            self.__adls_obj.upload_big_file(box_name, file_path, key)

    def check_record_exists(self, box_name: str, key: str) -> bool:
        """
        method to check if record exists in box
        :param box_name: box name str
        :param key: file name str
        :return: bool
        """
        if self.__s3_obj:
            return self.__s3_obj.check_if_file_exists(bucket_name=box_name, key=key)
        elif self.__adls_obj:
            return self.__adls_obj.check_if_file_exists(container_name=box_name, key=key)

    def get_signatured_record_url(self, key: str, box_name: str, expire_in: int = 7200, action: str = "READ") -> str:
        """
        method to get signatured url for record with expiry time setter
        :param key: file name str
        :param box_name: box name str
        :param expire_in: default is 7200 i.e (2 hours)
        :param action: "READ" or "WRITE" default="READ"
        :return: url str
        """
        if action not in PRESIGNED_URL_METHODS:
            raise ValueError("action must be in :" + ",".join(PRESIGNED_URL_METHODS.keys()))
        if self.__s3_obj:
            return self.__s3_obj.generate_presigned_url(box_name, key, action, expire_in)
        elif self.__adls_obj:
            return self.__adls_obj.get_blob_sas(box_name, key, action, expire_in)

    def get_record_from_box(self, box_name: str, key: str):
        """
        method to get record from box
        :param box_name: box name str
        :param key: file name str
        :return: file
        """
        if self.__s3_obj:
            return self.__s3_obj.get_object(bucket_name=box_name, key=key)
        elif self.__adls_obj:
            return self.__adls_obj.download_file(container_name=box_name, key=key)

    def get_records_from_dir(self, box_name, key) -> list:
        """
        method to get directory level records (all records in a directory)
        :param box_name: box name str
        :param key: file name str
        :return: list
        """
        if self.__s3_obj:
            return self.__s3_obj.get_files_from_directory(box_name, key)
        elif self.__adls_obj:
            return self.__adls_obj.list_blobs_in_container(box_name)

    def put_record_in_box(self, box_name, key, configs) -> None:
        """
        method to update data in record
        :param box_name: box name str
        :param key: file name str
        :param configs: configs to update
        :return: None
        """
        if self.__s3_obj:
            return self.__s3_obj.put_object(box_name, key, configs)
        elif self.__adls_obj:
            pass
