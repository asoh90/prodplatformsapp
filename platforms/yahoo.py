import json
import variables
import requests
import write_excel
import pandas as pd
import os
import sys
import codecs
import datetime
import numpy

topdir = os.path.join(os.path.dirname(__file__),".")
sys.path.append(topdir)
from requests_oauthlib import OAuth1

# Login credentials
key = None
secret = None
SIGNATURE_HMAC = "HMAC-SHA1"

SHEET_NAME = "Yahoo"

# URL
url = None

# Global output file
output = None

# Add Taxo Fixed Information
EXTENSIONS = {"urnType":"testid"}
METADATA = {"description":"Eyeota Taxonomy"}
GDPR_MODE = "oath_is_processor"

METADATA_FILE = "upload/metadata.json"
DATA_FILE = "upload/data.json"

def callAPI(platform, function, file_path):
    try:
        global url
        global key
        global secret
        if platform == "Yahoo":
            url = "https://datax.yahooapis.com/v1/taxonomy"
            key = variables.login_credentials['Yahoo']['Key']
            secret = variables.login_credentials['Yahoo']['Secret']
        elif platform == "Yahoo Staging":
            url = "https://sandbox.datax.yahooapis.com/v1/taxonomy"
            key = variables.login_credentials['Yahoo-Staging']['Key']
            secret = variables.login_credentials['Yahoo-Staging']['Secret']
        else:
            return {"message":"ERROR: Platform '{}' is incorrect!".format(platform)}
    except:
        return {"message":"ERROR: Incorrect login credentials! Please download 'asoh-flask-deploy.sh' file from <a href='https://eyeota.atlassian.net/wiki/pages/viewpageattachments.action?pageId=127336529&metadataLink=true'>Confluence</a> again!>"}

    output = "ERROR: option is not available"
    if (function == "Refresh Segments"):
        # Check if SHEET_NAME exists in uploaded file
        try:
            read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
        except:
            return{'message':"ERROR: Unable to find sheet name: {}".format(SHEET_NAME)}
            
        output = read_file_to_add_segments(file_path)
    elif (function == "Query All Segments"):
        output = get_query_all()
    return output

# Authenticate using oauthlib
def authenticate():
    global key
    global secret
    oauth = OAuth1(key,
                client_secret=secret,
                signature_method=SIGNATURE_HMAC)
    return oauth

def read_child_segment(parent_segment, json_file):
    global output
    if "id" in json_file:
        segment_id = json_file['id']

        if "description" in json_file:
            # output.append(json_file['id'] + "|" + parent_segment + "|" + json_file['description'])
            output[segment_id] = {
                "name":parent_segment,
                "description":json_file['description']
            }
        else:
            # output.append(json_file['id'] + "|" + parent_segment + "|")
            output[segment_id] = {
                "name":parent_segment,
                "description":None
            }

        if "users" in json_file:
            segment_users = json_file["users"]
            buyers_list = segment_users["include"]

            buyer_string = ""
            for buyer in buyers_list:
                if len(buyer_string) == 0:
                    buyer_string = buyer
                else:
                    buyer_string = buyer_string + "|" + buyer

            output[segment_id]["private_client_id"] = buyer_string
        else:
            output[segment_id]["private_client_id"] = None

    if "subTaxonomy" in json_file:
        child_segment_list = json_file["subTaxonomy"]
        for child_segment in child_segment_list:
            read_child_segment(parent_segment + " - " + child_segment['name'], child_segment)

def get_query_all():
    global output; output = {}
    global url
    write_name = []
    write_description = []
    write_id = []
    write_private_client_id = []

    query_response = None

    try:
        oauth = authenticate()
        if (oauth == None):
            return{'message':"ERROR: authenticating Yahoo API. Please check .sh file if credentials are correct."}
        request_to_send = requests.get(url=url,
                        auth=oauth)
        print("Query Request: {}".format(request_to_send.url))
        variables.logger.warning("{} Query Request: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        query_response = request_to_send.json()
        # print("Query Response: {}".format(query_response))

        for segment in query_response:
            read_child_segment(segment['name'], segment)

        for segment_id in output:
            output_segment = output[segment_id]
            write_id.append(segment_id)
            write_name.append(output_segment["name"])
            write_description.append(output_segment["description"])
            write_private_client_id.append(output_segment["private_client_id"])
            # output_list = row.split("|")
            # write_id.append(output_list[0])
            # write_name.append(output_list[1])
            # write_description.append(output_list[2])

        write_df = pd.DataFrame({
            "Segment ID":write_id,
            "Segment Name":write_name,
            "Segment Description":write_description,
            "Private Client ID":write_private_client_id
        })

        return write_excel.write(write_df, "DONOTUPLOAD_Yahoo_" + "Query", SHEET_NAME)
    except:
        if not query_response is None:
            return {"message":"ERROR: " + query_response["message"]}
        else:
            return {"message":"ERROR: Unknown error running function for Yahoo"}
            
def split_segments_to_add(segment_dict, segment_name_list, segment_id, segment_description, private_client_id):
    current_segment_name = segment_name_list[0]
    segment_name_list = segment_name_list[1:]

    # current_segment_name already in segment_dict
    if current_segment_name in segment_dict:
        # current_segment is the lowest child segment
        if len(segment_name_list) == 0:
            segment_dict[current_segment_name]["id"] = int(segment_id)
        # current_segment_name is not the lowest child segment
        else:
            # current_segment_name is already a parent in segment_dict
            if "subTaxonomy" in segment_dict[current_segment_name]:
                temp_segment_dict = segment_dict[current_segment_name]["subTaxonomy"]
                segment_dict[current_segment_name]["subTaxonomy"] = split_segments_to_add(temp_segment_dict, segment_name_list, segment_id, segment_description, private_client_id)
            else:
                temp_subTaxonomy = split_segments_to_add({}, segment_name_list, segment_id, segment_description, private_client_id)
                segment_dict[current_segment_name]["subTaxonomy"] = temp_subTaxonomy
    # current_segment_name is not a parent in segment_dict
    else:
        # current_segment is the lowest child segment
        if len(segment_name_list) == 0:
            segment_dict[current_segment_name] = {"id":int(segment_id),"description":str(segment_description), "private_client_id":private_client_id}
        # current_segment_name is not the lowest child segment
        else:
            temp_subTaxonomy = split_segments_to_add({}, segment_name_list, segment_id, segment_description, private_client_id)
            segment_dict[current_segment_name] = {"subTaxonomy":temp_subTaxonomy}

    return segment_dict

# Returns a json file to be sent to Yahoo API
def format_segment_json(segment_dict):
    # {"name": "AU CoreLogic RP Data", "type": "SEGMENT", "targetable": "false", "subTaxonomy": [
        # {"name": "Real Estate Indicator", "type": "SEGMENT", "targetable": "false", "subTaxonomy": [
    # print(segment_dict)
    data = []

    for segment_name in segment_dict:
        # is most child segment
        new_dict = {}
        if "id" in segment_dict[segment_name]:
            new_dict["name"] = segment_name
            new_dict["id"] = segment_dict[segment_name]["id"]
            new_dict["description"] = segment_dict[segment_name]["description"]
            new_dict["gdpr_mode"] = GDPR_MODE
            new_dict["type"] = "SEGMENT"
            new_dict["targetable"] = True
            
            private_client_id = segment_dict[segment_name]["private_client_id"]
            if not private_client_id is None:
                private_client_id = str(private_client_id)
                private_client_id_list = private_client_id.split("|")
                new_dict["users"] = {"include":private_client_id_list}
        else:
            new_dict["name"] = segment_name
            new_dict["type"] = "SEGMENT"
            new_dict["targetable"] = False

        if "subTaxonomy" in segment_dict[segment_name]:
            new_dict["subTaxonomy"] = format_segment_json(segment_dict[segment_name]["subTaxonomy"])

        data.append(new_dict)

    return data

def read_file_to_add_segments(file_path):
    able_to_upload = True
    read_df = None
    try:
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1], encoding='utf-8')
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    segment_dict = {}
    temp_dict = None
    
    segment_name_list = read_df["Segment Name"]
    segment_id_list = read_df["Segment ID"]
    segment_description_list = read_df["Segment Description"]
    private_client_id_list = read_df["Private Client ID"]
    
    for row_num in range(len(segment_name_list)):
        segment_id = segment_id_list[row_num]
        segment_name = segment_name_list[row_num]
        segment_name_split = segment_name.split(" - ")
        segment_description = segment_description_list[row_num]
        private_client_id = private_client_id_list[row_num]
        try:
            private_client_id = str(int(private_client_id))
        except:
            if numpy.isnan(private_client_id):
                private_client_id = None

        segment_dict = split_segments_to_add(segment_dict, segment_name_split, segment_id, segment_description, private_client_id)
    
    data = None
    try:
        data = format_segment_json(segment_dict)
    except:
        variables.logger.warning("{} Please sort Yahoo taxonomy by Segment Name in desc order before uploading".format(datetime.datetime.now().isoformat()))
        return {"message": "ERROR: Please sort Yahoo taxonomy by Segment Name in desc order before uploading"}

    with open (METADATA_FILE, 'w') as fp:
        json.dump(METADATA, fp)

    with open (DATA_FILE, 'w') as fp:
        json.dump(data, fp)

    files = {'metadata':open(METADATA_FILE), 'data':open(DATA_FILE)}

    oauth = authenticate()
    requests_to_send = requests.post(url=url,
                                    auth=oauth,
                                    files=files)
    print("Query sent: {}".format(requests_to_send.url))
    variables.logger.warning("{} Query Sent: {}".format(datetime.datetime.now().isoformat(), requests_to_send.url))
    query_response = requests_to_send.json()
    refresh_segments_status_code = requests_to_send.status_code

    os.remove(file_path)
    os.remove(METADATA_FILE)
    os.remove(DATA_FILE)

    if refresh_segments_status_code == 202:
        variables.logger.warning("{} File has been uploaded. Please wait 1 hour to retrieve the updated segments.".format(datetime.datetime.now().isoformat()))
        return {"message": "File has been uploaded. Please wait 1 hour to retrieve the updated segments."}
    else:
        variables.logger.warning("{} Error {} {}".format(datetime.datetime.now().isoformat(), refresh_segments_status_code, query_response))
        return {"message": "Error {} {}".format(refresh_segments_status_code, query_response)}