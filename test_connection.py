import boto3

def check_s3_connection():
    # AWS credentials from your provided JSON
    aws_access_key_id = "INSERT KEY HERE"
    aws_secret_access_key = "INSERT KEY HERE"
    s3_bucket = "demodatapipelinecg96"
    s3_path = "demodatapipelinecg96/"

    try:
        # Create an S3 client
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # List objects in the bucket
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_path)

        # Check if the response contains objects
        if 'Contents' in response:
            print("Connected to S3 bucket successfully.")
            print("Objects in the bucket:")
            for obj in response['Contents']:
                print(f"- {obj['Key']}")
        else:
            print("Connected to S3 bucket, but no objects found.")
    
    except Exception as e:
        print(f"Error connecting to S3 bucket: {str(e)}")

if __name__ == "__main__":
    check_s3_connection()
