import requests
import variables
import json
import write_excel
import pandas as pd
import os
import numpy
import time
import datetime

# Authenticate credentials
AUTHENTICATE_URL = "https://auth.mediamath.com/oauth/token"
URL = "https://api.mediamath.com/"
SESSION_URL = URL + "api/v2.0/session"
API_URL = URL + "dmp/v2.0/"
GET_SEGMENTS_URL = API_URL + "audience_segments/"

CLIENT_ID = "IBxiUniDVrRYSdSXUHJgoq6KdJ7F5oN0"
CLIENT_SECRET = "NnU9qtfRtruQypo7e2QJh_as_HjlDjppZAhBP0wWeRkqdSzcVrZSln_8PdXrOn50"

SHEET_NAME = "MediaMath"
VENDOR_ID = 687

EYEOTA_TAXONOMY_ID = 100377
EYEOTA_TAXONOMY_AUDIENCE_VENDOR_ID = 16
EYEOTA_TAXONOMY_NAME = "Eyeota"
EYEOTA_TAXONOMY_ORGANIZATION = []

# EYEOTA_TAXONOMY_ID = 101667
# EYEOTA_TAXONOMY_AUDIENCE_VENDOR_ID = 3000
# EYEOTA_TAXONOMY_NAME = "Eyeota - (Private) -"
# EYEOTA_TAXONOMY_ORGANIZATION = [100048]

def callAPI(platform, function, file_path):
    if function == "Query All Segments":
        return get_all_segments()
    elif function == "Refresh Segments":
        return read_file_to_refresh_segments(file_path)

def authenticate():
    username = variables.login_credentials['MediaMath']['Login']
    password = variables.login_credentials['MediaMath']['PW']
    access_token = None

    auth_request = requests.post(AUTHENTICATE_URL,
                        headers={
                                'Content-Type':'application/json'
                        },
                        json={
                              'grant_type':'password',
                              'username':username,
                              'password':password,
                              'audience':URL,
                              'scope':'read',
                              'client_id':CLIENT_ID,
                              'client_secret':CLIENT_SECRET
                        })
    print("Authenticate URL: {}".format(auth_request.url))
    variables.logger.warning("{} Authenticate URL: {}".format(datetime.datetime.now().isoformat(), auth_request.url))

    if auth_request.status_code == 200:
        auth_response = auth_request.json()
        access_token = auth_response["access_token"]
    return access_token

def get_session(access_token):
    adama_session = None
    session_request = requests.get(SESSION_URL,
                        headers={
                            'Authorization':'Bearer ' + access_token,
                        })

    print("Session URL: {}".format(session_request.url))
    variables.logger.warning("{} Session URL: {}".format(datetime.datetime.now().isoformat(), session_request.url))

    if session_request.status_code == 200:
        session_cookies = session_request.cookies
        session_dict = session_cookies.get_dict()
        adama_session = session_dict["adama_session"]

    return adama_session

# def get_taxonomy_ids(access_token, session):
#     taxonomy_id_list = []
#     get_taxonomy_request = requests.get("https://api.mediamath.com/dmp/v2.0/audience_segments/",
#                             headers={
#                                 'Authorization':"Bearer " + access_token,
#                                 'Cookie': "adama_session=" + session,
#                                 'Content-Type':"application/json"
#                             })
#     print("Get Taxonomy Request: {}".format(get_taxonomy_request.url))

#     if get_taxonomy_request.status_code == 200:
#         get_taxonomy_response = get_taxonomy_request.json()
#         taxonomy_data = get_taxonomy_response["data"]

#         for taxonomy in taxonomy_data:
#             taxonomy_name = taxonomy["taxonomy"]["name"].lower()

#             if 'eyeota' in taxonomy_name:
#                 taxonomy_id = taxonomy["taxonomy_id"]
#                 taxonomy_id_list.append(taxonomy_id)

#     return taxonomy_id_list

def get_segments(access_token, session, taxonomy_id, segment_dict):
    get_segment_request = requests.get("https://api.mediamath.com/dmp/v2.0/audience_segments/" + str(taxonomy_id),
                            headers={
                                'Authorization':"Bearer " + access_token,
                                'Cookie': "adama_session=" + session,
                                'Content-Type':"application/json"
                            })
    print("Get Segment Request: {}".format(get_segment_request.url))
    variables.logger.warning("{} Get Segment Request: {}".format(datetime.datetime.now().isoformat(), get_segment_request.url))

    segment_raw_json = get_segment_request.json()
    segment_json = segment_raw_json["data"]

    parent_segment_name = ""
    segment_dict = process_segment_json(segment_json, segment_dict, parent_segment_name, None)

    return segment_dict

def process_segment_json(segment_json, segment_dict, parent_segment_name, taxonomy_info_dict):
    if "taxonomy" in segment_json:
        if taxonomy_info_dict == None:
            taxonomy_info_dict = {}

        updated_on = segment_json["updated_on"]
        segment_visibility = segment_json["visibility"]
        segment_revenue_share_pct = segment_json["revenue_share_pct"]
        segment_permissions_organizations = segment_json["permissions"]["organizations"]
        segment_permissions_agencies = segment_json["permissions"]["agencies"]
        segment_permissions_advertisers = segment_json["permissions"]["advertisers"]
        segment_taxonomy_json = segment_json["taxonomy"]

        taxonomy_info_dict = {
                                "updated_on":updated_on,
                                "visibility":segment_visibility,
                                "revenue_share_pct":segment_revenue_share_pct,
                                "organizations":segment_permissions_organizations,
                                "agencies":segment_permissions_agencies,
                                "advertisers":segment_permissions_advertisers
                            }

        segment_name = segment_taxonomy_json["name"]
        segment_id = segment_taxonomy_json["id"]

        segment_uniques = None
        if "uniques" in segment_taxonomy_json:
            segment_uniques = segment_taxonomy_json["uniques"]
        segment_retail_cpm = None
        if "retail_cpm" in segment_taxonomy_json:
            segment_retail_cpm = segment_taxonomy_json["retail_cpm"]
        segment_code = None
        if "code" in segment_taxonomy_json:
            segment_code = segment_taxonomy_json["code"]
        segment_buyable = None
        if "buyable" in segment_taxonomy_json:
            segment_buyable = segment_taxonomy_json["buyable"]
        segment_wholesale_cpm = None
        if "wholesale_cpm" in segment_taxonomy_json:
            segment_wholesale_cpm = segment_taxonomy_json["wholesale_cpm"]
        segment_permissions_organizations = segment_json["permissions"]["organizations"]
        segment_permissions_agencies = segment_json["permissions"]["agencies"]
        segment_permissions_advertisers = segment_json["permissions"]["advertisers"]

        segment_dict[segment_name] = {
                                            "uniques":segment_uniques,
                                            "id":segment_id,
                                            "retail_cpm":segment_retail_cpm,
                                            "code":segment_code,
                                            "buyable":segment_buyable,
                                            "wholesale_cpm":segment_wholesale_cpm,
                                            "updated_on":updated_on,
                                            "visibility": segment_visibility,
                                            "revenue_share_pct":segment_revenue_share_pct,
                                            "organization_permissions":segment_permissions_organizations,
                                            "agencies_permissions":segment_permissions_agencies,
                                            "advertisers_permissions":segment_permissions_advertisers
                                        }
        segment_dict = process_segment_json(segment_taxonomy_json["children"], segment_dict, segment_name, taxonomy_info_dict)

    else:
        for segment in segment_json:
            if "children" in segment:
                current_parent_segment_name = parent_segment_name + " - " + segment["name"]
                segment_dict = process_segment_json(segment["children"], segment_dict, current_parent_segment_name, taxonomy_info_dict)

            segment_uniques = None
            if "uniques" in segment:
                segment_uniques = segment["uniques"]
            segment_name = parent_segment_name + " - " + segment["name"]
            segment_id = segment["id"]

            segment_retail_cpm = None
            if "retail_cpm" in segment:
                segment_retail_cpm = segment["retail_cpm"]
            segment_code = None
            if "code" in segment:
                segment_code = segment["code"]
            segment_buyable = segment["buyable"]
            segment_wholesale_cpm = None
            if "wholesale_cpm" in segment:
                segment_wholesale_cpm = segment["wholesale_cpm"]
            updated_on = taxonomy_info_dict["updated_on"]
            visibility = taxonomy_info_dict["visibility"]
            segment_revenue_share_pct = taxonomy_info_dict["revenue_share_pct"]
            segment_visibility = taxonomy_info_dict["visibility"]
            segment_permissions_organizations = taxonomy_info_dict["organizations"]
            segment_permissions_agencies = taxonomy_info_dict["agencies"]
            segment_permissions_advertisers = taxonomy_info_dict["advertisers"]


            segment_dict[segment_name] = {
                                            "uniques":segment_uniques,
                                            "id":segment_id,
                                            "retail_cpm":segment_retail_cpm,
                                            "code":segment_code,
                                            "buyable":segment_buyable,
                                            "wholesale_cpm":segment_wholesale_cpm,
                                            "updated_on":updated_on,
                                            "visibility": segment_visibility,
                                            "revenue_share_pct":segment_revenue_share_pct,
                                            "organization_permissions":segment_permissions_organizations,
                                            "agencies_permissions":segment_permissions_agencies,
                                            "advertisers_permissions":segment_permissions_advertisers
                                        }

    return segment_dict

def get_all_segments():
    access_token = authenticate()
    session = get_session(access_token)

    segment_dict = {}
    # taxonomy_id_list = get_taxonomy_ids(access_token, session)
    # for taxonomy_id in taxonomy_id_list:
    #     segment_dict = get_segments(access_token, session, taxonomy_id, segment_dict)

    segment_dict = get_segments(access_token, session, EYEOTA_TAXONOMY_ID, segment_dict)
    segment_key_list = segment_dict.keys()

    uniques_list = []
    segment_id_list = []
    segment_name_list = []
    segment_retail_cpm_list = []
    segment_code_list = []
    segment_buyable_list = []
    segment_wholesale_cpm_list = []
    segment_updated_on_list = []
    segment_visibility_list = []
    segment_revenue_share_pct_list = []
    segment_organization_permission_list = []
    segment_agencies_permissions_list = []
    segmetn_advertisers_permissions_list = []

    for segment_name in segment_key_list:
        segment = segment_dict[segment_name]

        segment_uniques = segment["uniques"]
        segment_retail_cpm = segment["retail_cpm"]
        segment_id = segment["id"]
        segment_code = segment["code"]
        segment_buyable = segment["buyable"]
        segment_wholesale_cpm = segment["wholesale_cpm"]
        segment_updated_on = segment["updated_on"]
        segment_visibility = segment["visibility"]
        segment_revenue_share_pct = segment["revenue_share_pct"]
        segment_organization_permissions = segment["organization_permissions"]
        segment_agencies_permissions = segment["agencies_permissions"]
        segment_advertisers_permissions = segment["advertisers_permissions"]

        uniques_list.append(segment_uniques)
        segment_id_list.append(segment_id)
        segment_name_list.append(segment_name)
        segment_retail_cpm_list.append(segment_retail_cpm)
        segment_code_list.append(segment_code)
        segment_buyable_list.append(segment_buyable)
        segment_wholesale_cpm_list.append(segment_wholesale_cpm)
        segment_updated_on_list.append(segment_updated_on)
        segment_visibility_list.append(segment_visibility)
        segment_revenue_share_pct_list.append(segment_revenue_share_pct)
        segment_organization_permission_list.append(segment_organization_permissions)
        segment_agencies_permissions_list.append(segment_agencies_permissions)
        segmetn_advertisers_permissions_list.append(segment_advertisers_permissions)

    write_df = pd.DataFrame({
        "mediamath_segment_id":segment_id_list,
        "segment_name":segment_name_list,
        "eyeota_segment_id":segment_code_list,
        "buyable":segment_buyable_list,
        "retail_cpm":segment_retail_cpm_list,
        "wholesale_cpm":segment_wholesale_cpm_list,
        "uniques":uniques_list,
        "updated_on":segment_updated_on_list,
        "visibility":segment_visibility_list,
        "revenue_share_pct":segment_revenue_share_pct_list,
        "organization_permissions":segment_organization_permission_list,
        "agencies_permissions":segment_agencies_permissions_list,
        "advertisers_permissions":segmetn_advertisers_permissions_list
    })

    return write_excel.write_and_email(write_df, "DONOTUPLOAD_MediaMath_query_all", SHEET_NAME)
    # return write_excel.write(write_df, "DONOTUPLOAD_MediaMath_query_all", SHEET_NAME)

def read_file_to_refresh_segments(file_path):
    access_token = authenticate()
    session = get_session(access_token)

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    segment_name_list = read_df["Segment Name"]
    segment_code_list = read_df["code"]
    segment_uniques_list = read_df["uniques"]
    segment_wholesale_cpm_list = read_df["Wholesale CPM"]
    segment_retail_cpm_list = read_df["Retail CPM"]
    segment_buyable_list = read_df["Buyable"]
    
    segment_raw_dict = {}
    
    for row_num in range(len(segment_name_list)):
        segment_name = segment_name_list[row_num]
        segment_code = segment_code_list[row_num]
        segment_uniques = segment_uniques_list[row_num]
        segment_wholesale_cpm = segment_wholesale_cpm_list[row_num]
        segment_retail_cpm = segment_retail_cpm_list[row_num]
        segment_buyable = segment_buyable_list[row_num]

        # print("Segment Name: {}".format(segment_name))

        segment_name_split = segment_name.split(" - ")
        segment_raw_dict = format_segment_raw_dict(segment_raw_dict, segment_name_split, segment_code, segment_uniques, segment_wholesale_cpm, segment_retail_cpm, segment_buyable)

    taxonomy_children_list = format_segment_dict(segment_raw_dict)
    segment_to_refresh_dict = {
                                "permissions":{
                                    "advertisers":[],
                                    "agencies":[],
                                    "organizations":EYEOTA_TAXONOMY_ORGANIZATION
                                },
                                "taxonomy":{
                                    "children":taxonomy_children_list,
                                    "name":EYEOTA_TAXONOMY_NAME
                                },
                                "audience_vendor_id":EYEOTA_TAXONOMY_AUDIENCE_VENDOR_ID
                            }
    output = refresh_segments(access_token, session, segment_to_refresh_dict)
    return output

def format_segment_raw_dict(segment_raw_dict, segment_name_split, segment_code, segment_uniques, segment_wholesale_cpm, segment_retail_cpm, segment_buyable):
    segment_partial_name = segment_name_split.pop(0)

    if len(segment_name_split) == 0:
        if not segment_partial_name in segment_raw_dict:
            if segment_buyable:
                temp_dict = {
                    "name":segment_partial_name,
                    "code":segment_code,
                    "children":{},
                    "buyable":segment_buyable,
                    "retail_cpm":segment_retail_cpm,
                    "wholesale_cpm":segment_wholesale_cpm,
                    "uniques":segment_uniques
                }
            else:
                temp_dict = {
                    "name":segment_partial_name,
                    "children":{},
                    "buyable":segment_buyable,
                    "retail_cpm":0
                }
        else:
            temp_dict = segment_raw_dict[segment_partial_name]
            temp_dict["name"] = segment_partial_name
            temp_dict["buyable"] = segment_buyable   
            if segment_buyable:
                temp_dict["code"] = segment_code                                           
                temp_dict["retail_cpm"] = segment_retail_cpm
                temp_dict["wholesale_cpm"] = segment_wholesale_cpm
                temp_dict["uniques"] = segment_uniques
            else:                                          
                temp_dict["retail_cpm"] = 0
        segment_raw_dict[segment_partial_name] = temp_dict
        return segment_raw_dict

    # segment partial name does not exist in segment_raw_dict
    if not segment_partial_name in segment_raw_dict:
        temp_dict = format_segment_raw_dict({}, segment_name_split, segment_code, segment_uniques, segment_wholesale_cpm, segment_retail_cpm, segment_buyable)
        segment_raw_dict[segment_partial_name] = {
                                                    "name":segment_partial_name,
                                                    'children':temp_dict,
                                                    'buyable':False,
                                                    'retail_cpm':0
                                                }
    else:
        partial_name_dict = segment_raw_dict[segment_partial_name]["children"]
        temp_dict = format_segment_raw_dict(partial_name_dict, segment_name_split, segment_code, segment_uniques, segment_wholesale_cpm, segment_retail_cpm, segment_buyable)
        segment_raw_dict[segment_partial_name]["children"] = temp_dict

    return segment_raw_dict

def format_segment_dict(segment_raw_dict):
    list_to_return = []
    for segment_partial_name in segment_raw_dict:
        current_segment = segment_raw_dict[segment_partial_name]

        current_segment_name = current_segment["name"]
        current_segment_code = None
        if "code" in current_segment:
            current_segment_code = str(current_segment["code"])
            if len(current_segment_code) > 0:
                current_segment_code = str(int(current_segment["code"]))
        current_segment_children_raw = current_segment["children"]
        current_segment_children = format_segment_dict(current_segment_children_raw)
        current_segment_buyable = current_segment["buyable"]
        if current_segment_buyable:
            current_segment_buyable = True
        else:
            current_segment_buyable = False
        current_segment_retail_cpm = int(current_segment["retail_cpm"])
        current_segment_wholesale_cpm = None
        if "wholesale_cpm" in current_segment:
            current_segment_wholesale_cpm = int(current_segment["wholesale_cpm"])
        current_segment_uniques = None
        if "uniques" in current_segment:
            current_segment_uniques = int(current_segment["uniques"])

        current_segment_dict = None
        if current_segment_buyable:
            current_segment_dict = {
                                        "name":current_segment_name,
                                        "children":current_segment_children,
                                        "buyable":current_segment_buyable,
                                        "code":current_segment_code,
                                        "retail_cpm":current_segment_retail_cpm,
                                        "wholesale_cpm":current_segment_wholesale_cpm,
                                        "uniques":current_segment_uniques
                                    }
        else:
            current_segment_dict = {
                                        "name":current_segment_name,
                                        "children":current_segment_children,
                                        "buyable":current_segment_buyable,
                                        "retail_cpm":current_segment_retail_cpm
                                    }
        list_to_return.append(current_segment_dict)

    return list_to_return

def refresh_segments(access_token, session, segment_dict):
    refresh_segment_request = requests.post("https://api.mediamath.com/dmp/v2.0/audience_segments/" + str(EYEOTA_TAXONOMY_ID),
                            headers={
                                'Authorization':"Bearer " + access_token,
                                'Cookie': "adama_session=" + session,
                                'Content-Type':"application/json"
                            },
                            data=json.dumps(segment_dict))
    print("Refresh Segment Request: {}".format(refresh_segment_request.url))
    variables.logger.warning("{} Refresh Segment Request: {}".format(datetime.datetime.now().isoformat(), refresh_segment_request.url))

    # print(json.dumps(segment_dict))

    response_status_code = refresh_segment_request.status_code
    response_json = refresh_segment_request.json()
    # print(response_json)

    if response_status_code == 202:
        return get_all_segments()
    else:
        return {"message": "Error: {}".format(response_json["errors"])}
        variables.logger.warning("{} ERROR: {}".format(datetime.datetime.now().isoformat(), response_json))