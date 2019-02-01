import pandas as pd
import os
import sys
import time
import variables
from datetime import datetime
import zipfile
import boto3
import datetime
import psycopg2
import postgres_script as pgs
from flask import session
import asyncio
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

OUTPUT_PATH = variables.RETURN_FOLDER + "/"
S3_URL = variables.S3_URL
AWS_KEY = variables.AWS_KEY
AWS_SECRET = variables.AWS_SECRET

# Asyncio loop
loop = asyncio.get_event_loop()

# Email credentials:
CURRENT_URL = variables.CURRENT_URL

S3_LIMIT_RECORDS = 2

def connect_s3_client():
    s3 = boto3.client('s3', endpoint_url=S3_URL,
            aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
    return s3

def connect_s3_resource():
    s3 = boto3.resource('s3', endpoint_url=S3_URL,
            aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
    return s3

def write(write_df, file_name, platform):
    now = datetime.datetime.now()
    now_format = now.strftime("%Y%m%d%H%M%S")

    file_name = file_name + "_" + now_format + ".xlsx"
    writer = pd.ExcelWriter(OUTPUT_PATH + file_name)
    write_df.to_excel(writer,'Sheet1',index=False)
    writer.save()

    # return {"message":"{} has been downloaded".format(file_name),
    #         "file":OUTPUT_PATH + file_name}

    # store file in s3 bucket
    s3 = connect_s3_client()
    # today = datetime.date.today()
    # bucket name is in YYYY_MM format
    bucket_name = now.strftime("%Y_%m")
    # if bucket exists, returns bucket, else creates
    bucket = s3.create_bucket(Bucket=bucket_name)

    s3.upload_file(OUTPUT_PATH + file_name, bucket_name, file_name)
    delete(OUTPUT_PATH, file_name)

    file_link = s3.generate_presigned_url('get_object', Params={'Bucket':bucket_name, 'Key':file_name}, ExpiresIn=36000)

    # store the file name and bucket in postgres
    try:
        conn, cursor = postgres_connect()

        now_datetime = datetime.datetime.now()
        now = now_datetime.strftime("")

        # check number of records in s3_file_match_new. If is 1000, move all records to s3_file_match_old
        cursor.execute(pgs.COUNT_RECORDS_QUERY)
        s3_records_list = cursor.fetchone()
        number_of_record = s3_records_list[0]

        if number_of_record >= S3_LIMIT_RECORDS:
            cursor.execute(pgs.DELETE_ID_QUERY.format(S3_LIMIT_RECORDS))

        # Insert into s3_file_match_new
        cursor.execute(pgs.INSERT_QUERY.format(bucket_name, file_name, platform, session["email"]))
        conn.commit()

        cursor.execute(pgs.GET_ID_QUERY.format(bucket_name, file_name))
        s3_match_id_list = cursor.fetchone()
        if not s3_match_id_list is None:
            s3_match_id = s3_match_id_list[0]
        else:
            cursor.execute(pgs.GET_ID_QUERY.format(bucket_name, file_name))
            s3_match_id = s3_match_id_list[0]
        
    except Exception as e:
        return {"message": "ERROR: {}".format(e),
                "result":e,
                "status":"Error"}

    return {"message":"Click on the file name to download your file: <a href='{}'>{}</a>".format("/return_output?id=" + str(s3_match_id), file_name),
            "result":str(s3_match_id),
            "status":"OK"}
    # ----end s3 test----

def write_and_email(write_df, file_name, platform):
    user_email = session["email"]
    
    write_output = write(write_df, file_name, platform)
    write_output_status = write_output["status"]
    write_output_result = write_output["result"]
    
    return email(write_output_status, write_output_result, file_name)

def run_async(write_df, file_name):
    loop.create_task(write_async(write_df, file_name))
    # loop.run_forever()
    return {"message":"Downloaded results will be emailed to {}".format(session["email"])}

def delete(file_path, file_name):
    return_filelist = [return_file for return_file in os.listdir(file_path) if return_file.endswith(".xlsx")]
    for return_file in return_filelist:
        if (return_file == file_name):
            os.remove(os.path.join(OUTPUT_PATH, return_file))

def write_without_return(write_df, file_name):
    now = datetime.datetime.now()
    now_format = now.strftime("%Y%m%d%H%M%S")

    file_name = file_name + "_" + now_format + ".xlsx"
    writer = pd.ExcelWriter(OUTPUT_PATH + file_name)
    write_df.to_excel(writer,'Sheet1',index=False)
    writer.save()

    # Returns with .xlsx extension, without OUTPUT_PATH
    return file_name

def write_zip_file(file_names, platform, zipped_file_name):
    now = datetime.datetime.now()
    now_format = now.strftime("%Y%m%d%H%M%S")

    file_name = zipped_file_name + "_" + now_format + ".zip"
    zipf = zipfile.ZipFile(OUTPUT_PATH + file_name, 'w', zipfile.ZIP_DEFLATED)
    for each_file_name in file_names:
        each_file_name = OUTPUT_PATH + each_file_name
        zipf.write(each_file_name)
    zipf.close()

    # store file in s3 bucket
    s3 = connect_s3_client()
    today = datetime.date.today()
    # bucket name is in YYYY_MM format
    bucket_name = today.strftime("%Y_%m")
    # if bucket exists, returns bucket, else creates
    bucket = s3.create_bucket(Bucket=bucket_name)
    
    s3.upload_file(OUTPUT_PATH + file_name, bucket_name, file_name)
    delete(OUTPUT_PATH, file_name)

    file_link = s3.generate_presigned_url('get_object', Params={'Bucket':bucket_name, 'Key':file_name}, ExpiresIn=36000)

    # store the file name and bucket in postgres
    try:
        conn, cursor = postgres_connect()

        now_datetime = datetime.datetime.now()
        now = now_datetime.strftime("")

        # check number of records in s3_file_match_new. If is 1000, move all records to s3_file_match_old
        cursor.execute(pgs.COUNT_RECORDS_QUERY)
        s3_records_list = cursor.fetchone()
        number_of_record = s3_records_list[0]

        if number_of_record >= S3_LIMIT_RECORDS:
            cursor.execute(pgs.DELETE_ID_QUERY.format(S3_LIMIT_RECORDS))

        # Insert into s3_file_match_new
        cursor.execute(pgs.INSERT_QUERY.format(bucket_name, file_name, platform, session["email"]))
        conn.commit()

        cursor.execute(pgs.GET_ID_QUERY.format(bucket_name, file_name))
        s3_match_id_list = cursor.fetchone()
        if not s3_match_id_list is None:
            s3_match_id = s3_match_id_list[0]
        else:
            cursor.execute(pgs.GET_ID_QUERY.format(bucket_name, file_name))
            s3_matc
        
    except Exception as e:
        return {"message": "ERROR: {}".format(e)}

    return {"message":"Click on the file name to download your file: <a href='{}'>{}</a>".format("/return_output?id=" + str(s3_match_id), file_name),
            "result":str(s3_match_id),
            "status":"OK"}

def return_single_file(file_name, platform):
    # store file in s3 bucket
    s3 = connect_s3_client()
    today = datetime.date.today()
    # bucket name is in YYYY_MM format
    bucket_name = today.strftime("%Y_%m")
    # if bucket exists, returns bucket, else creates
    bucket = s3.create_bucket(Bucket=bucket_name)
    
    s3.upload_file(OUTPUT_PATH + file_name, bucket_name, file_name)
    delete(OUTPUT_PATH, file_name)

    file_link = s3.generate_presigned_url('get_object', Params={'Bucket':bucket_name, 'Key':file_name}, ExpiresIn=36000)

    # store the file name and bucket in postgres
    try:
        conn, cursor = postgres_connect()

        now_datetime = datetime.datetime.now()
        now = now_datetime.strftime("")

        # check number of records in s3_file_match_new. If is 1000, move all records to s3_file_match_old
        cursor.execute(pgs.COUNT_RECORDS_QUERY)
        s3_records_list = cursor.fetchone()
        number_of_record = s3_records_list[0]

        if number_of_record >= S3_LIMIT_RECORDS:
            cursor.execute(pgs.DELETE_ID_QUERY.format(S3_LIMIT_RECORDS))

        # Insert into s3_file_match_new
        cursor.execute(pgs.INSERT_QUERY.format(bucket_name, file_name, platform, session["email"]))
        conn.commit()

        cursor.execute(pgs.GET_ID_QUERY.format(bucket_name, file_name))
        s3_match_id_list = cursor.fetchone()
        if not s3_match_id_list is None:
            s3_match_id = s3_match_id_list[0]
        
    except Exception as e:
        return {"message": "ERROR: {}".format(e)}

    return {"message":"Click on the file name to download your file: <a href='{}'>{}</a>".format("/return_output?id=" + str(s3_match_id), file_name),
            "result":str(s3_match_id),
            "status":"OK"}


def return_report(file_names, platform, file_path):
    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    write_output = None
    # Has 1 or 0 file, return the actual file. Else, return a zipped file.
    if len(file_names) < 2:
        write_output = return_single_file(file_names[0], platform)
    else:
        write_output = write_zip_file(file_names, platform, "reports")
    
    write_output_status = write_output["status"]
    write_output_result = write_output["result"]
    
    return email(write_output_status, write_output_result, file_name)

def postgres_connect():
    connect_str = "dbname='{}' user='{}' host='{}' password='{}'".format(variables.POSTGRES_DB, variables.POSTGRES_USER, variables.POSTGRES_HOST, variables.POSTGRES_PASSWORD)
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()

    return conn, cursor

def get_s3_file_name(id):
    conn, cursor = postgres_connect()

    # Get bucket and file_name from s3_file_match_new
    cursor.execute(pgs.GET_FILE_NAME_QUERY.format(id))
    s3_file_name_list = cursor.fetchone()

    return s3_file_name_list[0]

def download_s3_file(id):
    delete_before_one_day_files()

    # Example: http://127.0.0.1:5000/return_output?id=1
    conn, cursor = postgres_connect()

    # Get bucket and file_name from s3_file_match_new
    cursor.execute(pgs.GET_LINK_QUERY.format(id))
    s3_info_list = cursor.fetchone()
    
    file_name = None
    bucket = None

    if not s3_info_list is None:
        bucket = s3_info_list[0]
        file_name = s3_info_list[1]
    # if None, then get bucket and file_name from s3_file_match_old
    else:
        cursor.execute(pgs.GET_LINK_QUERY.format(id))
        s3_info_list = cursor.fetchone()

        if not s3_info_list is None:
            bucket = s3_info_list[0]
            file_name = s3_info_list[1]

    if not file_name is None and not bucket is None:
        s3 = connect_s3_resource()
        download_output = s3.Bucket(bucket).download_file(file_name, OUTPUT_PATH + file_name)

    return file_name

# email message is the id if success, error message if failure
def email(status, message, file_name):
    gmail_user = variables.login_credentials['Email']['Login']
    gmail_password = variables.login_credentials['Email']['PW']

    recipient = session["email"]

    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = recipient

    if status == "OK":
        msg['Subject'] = "Platform API Application File Completed: {}".format(file_name)
        body = """\
        <html>
            <body>
                <p>
                    Your output file is completed.<br>
                    <a href="http://127.0.0.1:5000/return_output?id={}">Click here</a> to redirect to download the file.
                </p>
            </body>
        </html>
        """.format(message)
    else:
        msg['Subject'] = "Platform API Application File Error"
        body = """\
        <html>
            <body>
                <p>
                    Error downloading file. Error messsage as below:<br>
                    {}
                </p>
            </body>
        </html>
        """.format(message)
    msg.attach(MIMEText(body,'html'))

    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login(gmail_user, gmail_password)
    server.sendmail(gmail_user, recipient, msg.as_string())

    server.close()

    return {"message":"Downloaded results will be emailed to {}".format(session["email"])}

def delete_before_one_day_files():
    now = time.time()

    for f in os.listdir(OUTPUT_PATH):
        # 86400 = seconds per day
        # Delete all files that are created more than 1 day before
        if os.stat(os.path.join(OUTPUT_PATH,f)).st_mtime < now - 86400:
            os.remove(OUTPUT_PATH + f)