import boto3



def get_minio_client(
                     minio_endpoint,
                     minio_access_key,
                     minio_secret_key,
                     region_name,
                     type:str="s3"
                     ):
    
    
    
    s3 = boto3.client(
        type,
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name=region_name,
    )

    return s3