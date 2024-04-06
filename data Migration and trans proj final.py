import requests
import zipfile
import glob
import boto3
import os
import sys
import json
from pymongo import MongoClient

#TO CONNECT S3 BUCKETS
# AWS credentials
AWS_ACCESS_KEY_ID = 'AKIAW3MEDNI5DQXUA2UT'
AWS_SECRET_ACCESS_KEY = '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'

def download_zip_file(url, filename):
    r = requests.get(url, stream=True, headers={'User-Agent': 'Mozilla/5.0'})

    if r.status_code == 200:
        with open(filename, 'wb') as f:
            r.raw.decode_content = True

            f.write(r.content)
            print('Zip File Downloading Completed')
url = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
# filename = url.split('/')[-1]
SourceData = "companyfacts.zip"
download_zip_file(url, SourceData)


my_zip="/content/companyfacts.zip"
files_to_extract =["CIK0000001750.json","CIK0000001800.json","CIK0000001961.json","CIK0000002034.json","CIK0000002098.json"]

with zipfile.ZipFile(my_zip, 'r') as zipfile:
    for file in zipfile.namelist():
        if file in files_to_extract:
            zipfile.extract(file,"/content/Unzipthefiles")



# target location of the files on S3
S3_BUCKET_NAME = 'raghuaws'

#Location of source files
DATA_FILES_LOCATION = "/content/Unzipthefiles/CIK*.json"

s3_client = boto3.client("s3",aws_access_key_id = AWS_ACCESS_KEY_ID,aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

# The list of files we're uploading to S3
files = glob.glob(DATA_FILES_LOCATION)

try:
    file_num=1
    for file in files:
        s3_file = f"{os.path.basename(file)}"
        s3_client.upload_file(file, S3_BUCKET_NAME, s3_file)
        # print(f"File {file_num} of {len(files)} uploaded")
        file_num= file_num + 1
except:
    print("An unexpected error occured uploading the Data files to S3")
    sys.exit(-1)

# MongoDB connection details
connection = MongoClient('mongodb+srv://Mithra:SanndRtagh@cluster0.cntykjq.mongodb.net/?retryWrites=true&w=majority')  # Replace with your MongoDB connection string
db = connection['data_trans_proj']  #Creating Data base
youtube_collection = db['data_trans']  # Collection to store all YouTube data

def extract_json_from_s3(S3_BUCKET_NAME):
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    json_files = []
    # List all objects in the bucket
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
    for obj in response.get('Contents', []):
        key = obj['Key']
        # Check if the object is a JSON file
        if key.endswith('.json'):
            # Download JSON file
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            json_data = response['Body'].read().decode('utf-8')
            json_files.append(json.loads(json_data))
    return json_files

def load_json_into_mongodb(json_data):
    # Insert JSON data into MongoDB collection
    youtube_collection.insert_many(json_data)


 # Extract JSON files from S3
json_data = extract_json_from_s3(S3_BUCKET_NAME)
# Load JSON data into MongoDB
load_json_into_mongodb(json_data)


