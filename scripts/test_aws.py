# test_aws.py
import boto3

def test_aws_connection():
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        print("AWS Connection successful!")
        print("Available buckets:", [b['Name'] for b in response['Buckets']])
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    test_aws_connection()