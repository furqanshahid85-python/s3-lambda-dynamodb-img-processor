import csv
import time
import boto3
from PIL import Image
from uuid import uuid4
from PIL.ExifTags import TAGS

#uncomment to invoke configuration error

# import thirdPartyModule

tag_obj = {}
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table('dashbird-img-metadata-table')


def invoke_lambda_timeout():
    print('invoking lambda timeout...')
    time.sleep(30)


def invoke_lambda_out_of_memory():
    BUCKET = 'dashbird-misc-bucket'
    CSV_FILE_KEY = 'large_csv_file.csv'
    csv_file = s3_client.get_object(Bucket=BUCKET, Key=CSV_FILE_KEY)
    csvcontent = csv_file['Body'].read().split(b'\n')
    _ = csv.DictReader(csvcontent)


def lambda_handler(event, context):
    print(event)

    # uncomment to invoke timeout error
    # invoke_lambda_timeout()

    # uncomment to invoke out_of_memory error
    # invoke_lambda_out_of_memory()


    # getting bucket and object key from event object
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # creating download path and downloading the img object from S3
    img_download_path = '/tmp/{}'.format(key)

    with open(img_download_path, 'wb') as img_file:
        s3_client.download_fileobj(source_bucket, key, img_file)

    # Creating and saving img thumbnail from downloaded img
    image = Image.open(img_download_path)

    # extracting the exif metadata
    exifdata = image.getexif()

    tag_obj['imageId'] = str(uuid4())
    for tag_id in exifdata:
        # get the tag name, instead of human unreadable tag id
        tag = TAGS.get(tag_id, tag_id)
        data = exifdata.get(tag_id)

        # decode bytes
        if isinstance(data, bytes):
            data = data.decode()
        tag_obj[tag] = str(data)
        print(f"{tag:25}: {data}")
    
    # put item in dynamodb table
    response = ddb_table.put_item(Item=tag_obj)
    print(response)
