import boto3
from uuid import uuid4
from botocore.exceptions import NoCredentialsError


IMAGE = '<path-to-some-image>'
SOURCE_BUCKET = '<source-bucket-name>'

s3 = boto3.client('s3')

# no of events to generate from S3 to Lambda
x = 100

for i in range(x):
    try:
        s3.upload_file(IMAGE, SOURCE_BUCKET, 'img_{0}.jpg'.format(str(uuid4())))
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")
