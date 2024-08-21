import boto3

from typing import List

def get_s3_uris(s3_bucket:str, s3_prefix: str) -> List[str]:
    uris = []
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    contents = response["Contents"][1:]

    for file in contents:
        uri = f"s3://{s3_bucket}/" + file["Key"]
        uris.append(uri)
    return uris

