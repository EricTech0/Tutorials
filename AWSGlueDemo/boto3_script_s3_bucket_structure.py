import boto3
import json
import os

bucket_name = "eric-tech-data-source-s3-test-data"
region_name = "us-east-2"

####
# Use for create bucket folder structure in S3
# + S3
#   - eric-tech-data-source-s3-test-data <- Bucket name
#     - athena_query
#     - data
#       - sales_db
#         - transactions_csv
#           - ymd=20240101
#             - data_1200.csv
#         - treated_data
#     - scripts
#     - tmp
####

def output_boto3(obj):
    json_string = json.dumps(obj, indent=2, default=str)
    print(json_string)


def create_bucket(s3_client):
    response = s3_client.create_bucket(
        ACL="private",
        Bucket=bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": region_name,
        },
    )
    output_boto3(response)


def upload_file(s3_client, filename):
    s3_filename = "data/sales_db/transactions_csv/ymd=20240101/" + os.path.basename(
        filename
    )

    response = s3_client.upload_file(filename, bucket_name, s3_filename)
    output_boto3(response)


def make_folder(s3_client, keypath):
    result = s3_client.list_objects(Bucket=bucket_name, Prefix=keypath)
    if not "Contents" in result:
        s3_client.put_object(Bucket=bucket_name, Key=keypath)
    pass


def delete_bucket_force():
    s3_client = boto3.resource("s3")
    bucketClient = s3_client.Bucket(bucket_name)
    bucketClient.objects.all().delete()
    bucketClient.meta.client.delete_bucket(Bucket=bucket_name)
    pass


def main():
    s3_client = boto3.client("s3", region_name=region_name)

    create_bucket(s3_client)
    upload_file(s3_client, "data_1200.csv")
    make_folder(s3_client, "scripts/")
    make_folder(s3_client, "tmp/")
    make_folder(s3_client, "athena_query/")
    make_folder(s3_client, "data/sales_db/treated_data/")

    # delete_bucket_force()


if __name__ == "__main__":
    main()