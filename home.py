#!/usr/bin/python

from flask import Flask, render_template, url_for, flash, redirect, request, send_file, session, after_this_request
from flask_oauth import OAuth
from forms import SelectPlatformForm, SelectFunctionForm
from werkzeug.utils import secure_filename
import variables
import platform_manager as pm
import download_manager as dm
import os
import random, threading, webbrowser
import sys
from urllib.request import Request, urlopen, URLError
import write_excel
import json
import requests
import threadpool
import logging
from logging.handlers import RotatingFileHandler
from functools import wraps

# Site
SITE_URL = "127.0.0.1:5000"

# Google SSO Credentials
GOOGLE_CLIENT_ID = '696774976262-rg17k58uiani498vqkjdekfdunh7c6j3.apps.googleusercontent.com'
GOOGLE_CLIENT_SECRET = 'ceM7QWDLzwA-7xJ0qalIPweP'
REDIRECT_URI = "/oauth2callback"
DEBUG = True

app = Flask(__name__)
# app.debug = DEBUG
oauth = OAuth()

app.config['SECRET_KEY'] = variables.SECRET_KEY
app.config["UPLOAD_FOLDER"] = variables.UPLOAD_FOLDER
app.config["RETURN_FOLDER"] = variables.RETURN_FOLDER
# RETURN_FOLDER = variables.RETURN_FOLDER
UPLOAD_FOLDER = variables.UPLOAD_FOLDER

# default is expire in 1 hour: https://stackoverflow.com/questions/13851157/oauth2-and-google-api-access-token-expiration-time
google = oauth.remote_app('google',
                          base_url='https://www.google.com/accounts/',
                          authorize_url='https://accounts.google.com/o/oauth2/auth',
                          request_token_url=None,
                          request_token_params={'scope': 'https://www.googleapis.com/auth/userinfo.email',
                                                'response_type': 'code',
                                                'prompt':'consent',  # this field allows refresh_token to be generated every time
                                                'access_type':'offline'},
                          access_token_url='https://accounts.google.com/o/oauth2/token',
                          access_token_method='POST',
                          access_token_params={'grant_type': 'authorization_code'},
                          consumer_key=GOOGLE_CLIENT_ID,
                          consumer_secret=GOOGLE_CLIENT_SECRET)

def authenticate(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        # access_token = session.get('access_token')
        # # print("ACCESS TOKEN: {}".format(access_token))
        # if access_token is None:
        #     return redirect(url_for('login'))
        # access_token = access_token[0]
        
        # headers = {'Authorization': 'OAuth '+ access_token}
        # req = Request('https://www.googleapis.com/oauth2/v1/userinfo',
        #               None, headers)
        # try:
        #     res = urlopen(req)
        # except URLError as e:
        #     if e.code == 401:
        #         # Unauthorized - bad token
        #         session.pop('access_token', None)
        #         return redirect(url_for('login'))

        # login_response = res.read().decode('utf-8')
        # login_dict = json.loads(login_response)
        # session["email"] = login_dict["email"]
        check_output = check_login()
        if not check_output is None:
            return check_output
        else:
            return f(*args, **kwargs)
    return wrap

@app.route('/')
def index():
    return redirect(url_for('home'))

def check_login():
    access_token = session.get('access_token')
    # print("ACCESS TOKEN: {}".format(access_token))
    if access_token is None:
        return redirect(url_for('login'))
    access_token = access_token[0]
    
    headers = {'Authorization': 'OAuth '+ access_token}
    req = Request('https://www.googleapis.com/oauth2/v1/userinfo',
                  None, headers)
    try:
        res = urlopen(req)
    except URLError as e:
        if e.code == 401:
            # Unauthorized - bad token
            session.pop('access_token', None)
            return redirect(url_for('login'))

    login_response = res.read().decode('utf-8')
    login_dict = json.loads(login_response)
    session["email"] = login_dict["email"]
    return None
 
@app.route('/login')
def login():
    callback=url_for('authorized', _external=True)
    return google.authorize(callback=callback)

# get access token with refresh_token
def refresh(refresh_token):
    params = {
                'grant_type':'refresh_token',
                'client_id':GOOGLE_CLIENT_ID,
                'client_secret':GOOGLE_CLIENT_SECRET,
                'refresh_token':refresh_token
            }
    
    authorization_url = 'https://www.googleapis.com/oauth2/v4/token'

    refresh_request = requests.post(authorization_url, data=params)

    if refresh_request.ok:
        access_token = refresh_request.json()['access_token']
        # print("refresh access_token: {}".format(access_token))
        return access_token
    else:
        return None

@app.route(REDIRECT_URI)
@google.authorized_handler
def authorized(resp):
    # print("authorized: {}".format(resp))
    refresh_token = resp['refresh_token']
    access_token = refresh(refresh_token)
    session['access_token'] = access_token, ''
    return redirect(url_for('index'))

@google.tokengetter
def get_access_token():
    return session.get('access_token')

@app.route("/home", methods=['GET','POST'])
@authenticate
def home():
    check_output = check_login()
    if check_output == None:
        credentials = app.config.get('credentials')
        if credentials == None:
            render_template('error.html')
        
        variables.read_credentials(credentials)

        form = SelectPlatformForm()
        # if form.validate_on_submit():
        #     return redirect(url_for("function", platform=form.platform.data))
        platform_functions = variables.get_platform_functions(session["email"])
        return render_template('home.html', form=form, platform_functions=platform_functions)
    else:
        return check_output

# when file gets dropped into the dropzone, or "Query" function is selected, run this
@app.route("/process", methods=['GET','POST'])
@authenticate
def process():
    # get fields
    platform = request.form['platform']
    function = request.form['function']
    save_path = None

    if not "Query" in function:
        # Get downloaded file
        fileob = request.files["file"]
        filename = secure_filename(fileob.filename)
        save_path = "{}/{}".format(app.config["UPLOAD_FOLDER"], filename)
        fileob.save(save_path)

    output = pm.callAPI(platform, function, save_path)
    # print(output)

    # Old method to return the actual file
    # try:
    #     output_file = output["file"]
    #     response_output = send_file(RETURN_FOLDER + "/" + output_file, as_attachment=True, attachment_filename=output_file)
    #     response_output.headers['message'] = output["message"]
    #     return response_output
    # # If no file is returned, error message is returned instead
    # except:
    #     return output["message"]

    return output["message"]

@app.route("/downloaduploadtemplate", methods=['GET','POST'])
@authenticate
def download_upload_template():
    check_output = check_login()
    if check_output == None:
        return send_file("UploadTemplate.xlsx", as_attachment=True, attachment_filename="UploadTemplate.xlsx")
    else:
        return check_output

@app.route("/download_table", methods=['GET'])
@authenticate
def download_table():
    check_output = check_login()
    if check_output == None:
        records_list = dm.get_database_data()
        return render_template('download.html', records_list=records_list)
    else:
        return check_output

@app.route("/return_output", methods=['GET','POST'])
@authenticate
def return_output():
    check_output = check_login()
    if check_output == None:
        s3_id = request.args.get('id')
        file_name = write_excel.get_s3_file_name(s3_id)
        # return redirect(url_for('home', id=file_id))
        return redirect(url_for('home', id=s3_id, filename=file_name))
    else:
        return check_output

@app.route('/download_output/<id>')
@authenticate
def download_output(id):
    check_output = check_login()
    if check_output == None:
        file_name = write_excel.download_s3_file(id)
        return send_file(app.config["RETURN_FOLDER"] + "/" + file_name, as_attachment=True, attachment_filename=file_name)
    else:
        return check_output

# ------------------------------- Non route functions -----------------------------------------
# def delete_upload_and_to_return_files():
#     return_filelist = [return_file for return_file in os.listdir(RETURN_FOLDER) if return_file.endswith(".xlsx")]
#     for return_file in return_filelist:
#         os.remove(os.path.join(RETURN_FOLDER, return_file))

if __name__ == "__main__":
    # Create Pool for AppNexus
    appnexus_pool = threadpool.ThreadPool(15)
    variables.thread_pool_dict["AppNexus"] = appnexus_pool

    variables.logger = logging.getLogger('my_logger')
    handler = RotatingFileHandler('logs/my_log.txt', maxBytes=10000000000, backupCount=9999)
    variables.logger.addHandler(handler)

    app.config['credentials'] = sys.argv[1]
    port = 5000
    url = SITE_URL
    threading.Timer(1.25, lambda: webbrowser.open(url)).start()
    app.run(threaded=True, host='0.0.0.0')
