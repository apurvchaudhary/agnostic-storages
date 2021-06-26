import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

CHOICES = ["AWS", "AZURE"]

PRESIGNED_URL_METHODS = {
    "READ": "get_object",
    "WRITE": "put_object"
}

# size is in GB
S3_MULTIPART_SIZE = 1024 ** 3
# any file more than 1GB will be broken into multi parts and send concurrently
S3_LIST_CONFIG = TransferConfig(multipart_threshold=S3_MULTIPART_SIZE, max_concurrency=10,
                                multipart_chunksize=S3_MULTIPART_SIZE, use_threads=True)


class S3Storage:
    __client = None
    __region_name = None

    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.__region_name = region_name
        self.__client = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key,
                                     region_name=region_name)

    def list_buckets(self):
        return [bucket for bucket in self.__client.list_buckets()["Buckets"]]

    def create_bucket(self, bucket_name):
        self.__client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration={'LocationConstraint': self.__region_name})

    def get_object(self, bucket_name, key):
        return self.__client.get_object(Bucket=bucket_name, Key=key).get('Body')

    def list_file_in_bucket(self, bucket_name):
        return [file_name for file_name in self.__client.list_objects(Bucket=bucket_name)['Contents']]

    def upload_file(self, file_path, bucket_name, key):
        self.__client.upload_file(file_path, bucket_name, key)

    def upload_big_csv_with_configs(self, file_path, bucket_name, key, configs):
        self.__client.upload_file(file_path, bucket_name, key, ExtraArgs={'ContentType': 'text/csv'}, Config=configs)

    def generate_presigned_url(self, bucket_name, key_path, action, expires_in):
        return self.__client.generate_presigned_url(PRESIGNED_URL_METHODS[action],
                                                    Params={'Bucket': bucket_name, 'Key': key_path},
                                                    ExpiresIn=expires_in)

    def get_files_from_directory(self, bucket_name, key):
        return self.__client.list_objects_v2(Bucket=bucket_name, Prefix=key,
                                             Delimiter='/').get('Contents', [])

    def check_if_file_exists(self, bucket_name, key):
        try:
            self.__client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def put_object(self, bucket_name, key_path, configs):
        self.__client.put_object(Bucket=bucket_name, Body=configs, Key=key_path)


class StorageService:
    __access_key = None
    __secret_key = None
    __region_name = None
    __platform = None
    __s3_obj = None
    __adls_obj = None

    def __init__(self, access_key, secret_key, region_name, cloud_platform):
        if cloud_platform not in CHOICES:
            raise ValueError("cloud_platform must any of : " + ",".join(CHOICES))
        self.__platform = cloud_platform
        self.__access_key = access_key
        self.__secret_key = secret_key
        self.__region_name = region_name
        if cloud_platform == "AWS":
            self.__s3_obj = S3Storage(self.__access_key, self.__secret_key, self.__region_name)
        elif cloud_platform == "AZURE":
            self.__adls_obj = None

    def get_all_boxes(self):
        if self.__s3_obj:
            return self.__s3_obj.list_buckets()

    def create_box(self, box_name: str) -> None:
        if self.__s3_obj:
            return self.__s3_obj.create_bucket(bucket_name=box_name)

    def get_all_records_in_box(self, box_name: str) -> list:
        if self.__s3_obj:
            return self.__s3_obj.list_file_in_bucket(bucket_name=box_name)
        elif self.__adls_obj:
            pass

    def post_record_to_box(self, file_path: str, box_name: str, key: str) -> None:
        if self.__s3_obj:
            self.__s3_obj.upload_file(file_path, box_name, key)
        elif self.__adls_obj:
            pass

    def post_big_csv_record_to_box_with_configs(self, file_path: str, box_name: str,
                                                key: str, configs: TransferConfig=S3_LIST_CONFIG):
        if self.__s3_obj:
            self.__s3_obj.upload_big_csv_with_configs(file_path, box_name, key, configs)

    def check_record_exists(self, box_name: str, key: str) -> bool:
        if self.__s3_obj:
            return self.__s3_obj.check_if_file_exists(bucket_name=box_name, key=key)

    def get_signatured_record_url(self, key: str, box_name: str, expire_in=7200, action: str = "READ") -> str:
        if action not in PRESIGNED_URL_METHODS:
            raise ValueError("action must be in :" + ",".join(PRESIGNED_URL_METHODS.keys()))
        if self.__s3_obj:
            return self.__s3_obj.generate_presigned_url(box_name, key, action, expire_in)

    def get_record_from_box(self, box_name: str, key: str):
        if self.__s3_obj:
            return self.__s3_obj.get_object(bucket_name=box_name, key=key)

    def get_records_from_dir(self, box_name, key):
        if self.__s3_obj:
            return self.__s3_obj.get_files_from_directory(box_name, key)

    def put_record_in_box(self, box_name, key, configs):
        if self.__s3_obj:
            return self.__s3_obj.put_object(box_name, key, configs)
