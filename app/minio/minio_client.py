import boto3



def create_s3_bucket(s3, bucket_name: str, logger) -> None:
    
    response = s3.head_bucket(Bucket=bucket_name)
    print(response)
    
    if not response:

        s3.create_bucket(Bucket=bucket_name)
        if logger:
            logger.info(f"S3 bucket {bucket_name} created")
    else:
        if logger:
            logger.info(f"S3 bucket {bucket_name} already existing")
    
    return None
    


def delete_s3_file(s3, bucket_name: str, logger) -> None:

    
    response = s3.head_bucket(Bucket=bucket_name)
    print(response)
    
    if response:
        s3.delete_object(Bucket=bucket_name, key = None)
        logger.info(f"S3 bucket {bucket_name} deleted")
        
    return None



