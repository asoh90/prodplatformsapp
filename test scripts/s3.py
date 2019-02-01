# cd /Users/alvinsoh/Documents/Heroku/fake-s3-master/bin/
# sudo ./fakes3 -r /Users/alvinsoh/Documents/Heroku/fakes3_folders -p 8000 --license 169860572

import boto3

def main():
    s3 = boto3.client('s3', endpoint_url='http://localhost:8000', 
            aws_access_key_id='123', aws_secret_access_key='abc')

    s3.create_bucket(Bucket='AppNexus')
    s3.create_bucket(Bucket='Adobe_AAM')
    s3.create_bucket(Bucket='MediaMath')
    s3.create_bucket(Bucket='The_Trade_Desk')
    s3.create_bucket(Bucket='Yahoo')
    s3.create_bucket(Bucket='Adform')

    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]

    print("Bucket List: {}".format(buckets))

main()