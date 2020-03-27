import requests
import variables
import write_excel
import pandas as pd
import os
import numpy
import time
import datetime

URL = "https://api.demdex.com:443/"
# URL = "https://api-beta.demdex.com:443/"
AUTH_URL = URL + "oauth/token"
API_URL = URL + "v1/"
DATA_SOURCE_URL = API_URL + "datasources/"
DATA_FEED_URL = API_URL + "data-feeds/"
DATA_FEED_PLAN_URL = API_URL + "data-feeds/{}/plans/"
TRAIT_FOLDER_URL = API_URL + "folders/traits/"
TRAIT_URL = API_URL + "traits/"
TRAIT_RULE_URL = API_URL + "traits/{}"

SHEET_NAME = "Adobe AAM"
AUTHENTICATION_LIMIT_SECS = 3539

CONTENT_TYPE = "application/x-www-form-urlencoded"
AUTHORIZATION = "Basic ZXllb3RhLWJhYWFtOnJvZDZsOWluamRzZmwyN2E2cGUybjNsam50cmhndnRpM3A5NGN1YnUyMXVzdjZ2MXBicg=="
GRANT_TYPE = "password"

# constants to add data source
DATA_SOURCE_ID_TYPE = "COOKIE"
DATA_EXPORT_RESTRICTIONS = ['PII', 'ONSITE_PERSONALIZATION']
DATA_SOURCE_TYPE = "GENERAL"
ALLOW_DATA_SHARING = True
INBOUNDS2S = True
OUTBOUNDS2S = False

# constants to add data feed
ADD_DATA_FEED_DATA_BRANDING_TYPE = "BRANDED"
# Alvin, Derek, Don's account
ADD_DATA_FEED_CONTACT_USER_IDS = [120974,74130,135737,161329,185225]
ADD_DATA_FEED_DESCRIPTION = ""
# ADD_DATA_FEED_DISTRIBUTION = "PRIVATE"
ADD_DATA_FEED_BILLING = "ADOBE"
ADD_DATA_FEED_STATUS = "ACTIVE"  # ****TO CHANGE TO ACTIVE****

# constants to add data feed plan
ADD_DATA_FEED_PLAN_DESCRIPTION = ""
ADD_DATA_FEED_PLAN_BILLING_CYCLE = "MONTHLY_IN_ARREARS"
ADD_DATA_FEED_PLAN_STATUS = "ACTIVE"
ADD_DATA_FEED_PLAN_SEGMENT_AND_OVERLAP_BILLING_UNIT = "FIXED"

# constants to add trait
ADD_TRAIT_BACKFILL_STATUS = "NONE"
ADD_TRAIT_TYPE = 0
ADD_TRAIT_TRAIT_TYPE = "ON_BOARDED_TRAIT"
ADD_TRAIT_STATUS = "ACTIVE"   # ****TO CHANGE TO ACTIVE****

# constant to add trait folder
PID = 7784

# Login credentials
username = None
password = None

# Data Source ID to retrieve subscribers' contacts (currently using Eyeota Market - HK)
ZERO_SUBSCRIBERS_DATA_SOURCE_ID = 401922
GET_SUBSCRIBER_CONTACTS_URL = API_URL + "data-feeds/{}/potential-subscribers/".format(ZERO_SUBSCRIBERS_DATA_SOURCE_ID)

def callAPI(platform, function, file_path):
    try:
        global username; username = variables.login_credentials['AdobeAAM']['Login']
        global password; password = variables.login_credentials['AdobeAAM']['PW']
    except:
        return {"message":"ERROR: Incorrect login credentials! Please download 'asoh-flask-deploy.sh' file from <a href='https://eyeota.atlassian.net/wiki/pages/viewpageattachments.action?pageId=127336529&metadataLink=true'>Confluence</a> again!>"}

    output = {"message":"ERROR: option is not available"}

    if function == "Query All Segments":
        output = query_all_segments()
    elif function == "Query Subscriber Contacts":
        output = get_all_subscriber_contacts()
    elif function == "Get Trait Rule":
        output = read_all_to_get_trait_rule(file_path)
    else:
        # Check if SHEET_NAME exists in uploaded file
        try:
            read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
        except:
            return{'message':"ERROR: Unable to find sheet name: {}".format(SHEET_NAME)}

        if function == "Add Segments":
            output = read_all_to_add_segments(file_path)
        elif function == "Edit Segments":
            output = read_file_to_edit_segments(file_path)
        elif function == "Get Data Source Uniques":
            output = read_all_to_get_uniques_report(file_path)
    
    return output

def authenticate():
    data = {
                'grant_type':GRANT_TYPE,
                'username':username,
                'password':password
            }
    auth_json = requests.post(AUTH_URL,
                            headers={
                                'Content-Type':CONTENT_TYPE,
                                'Authorization':AUTHORIZATION
                            },
                            data=data).json()
    return auth_json["access_token"]

def get_subscriber_contacts(access_token):
    get_subscriber_contacts_request = requests.get(GET_SUBSCRIBER_CONTACTS_URL,
                                        headers={
                                            'Content-Type':"application/json",
                                            'Authorization':"Bearer " + access_token
                                        })

    print("Get Subscriber Contacts URL: {}".format(get_subscriber_contacts_request.url))
    variables.logger.warning("{} Get Subscriber Contacts URL: {}".format(datetime.datetime.now().isoformat(), get_subscriber_contacts_request.url))

    return access_token, get_subscriber_contacts_request.json()

def get_all_subscriber_contacts():
    access_token = authenticate()

    write_pid_list = []
    write_name_list = []
    write_contact_email_list = []
    write_contact_first_name_list = []
    write_contact_last_name_list = []

    subscriber_contacts_json = get_subscriber_contacts(access_token)
    # print(subscriber_contacts_json)

    for subscriber_contact in subscriber_contacts_json[1]:
        # print(subscriber_contact)
        pid = subscriber_contact["pid"]
        name = subscriber_contact["name"]
        contact_email = subscriber_contact["contactEmail"]
        contact_first_name = subscriber_contact["contactFirstName"]
        contact_last_name = subscriber_contact["contactLastName"]

        write_pid_list.append(pid)
        write_name_list.append(name)
        write_contact_email_list.append(contact_email)
        write_contact_first_name_list.append(contact_first_name)
        write_contact_last_name_list.append(contact_last_name)

    write_df = pd.DataFrame({
                    "Buyer PID":write_pid_list,
                    "Buyer Name":write_name_list,
                    "Buyer Email":write_contact_email_list,
                    "Buyer First Name":write_contact_first_name_list,
                    "Buyer Last Name":write_contact_last_name_list
                })
    return write_excel.write(write_df, "DONOTUPLOAD_AdobeAAM_query_subscriber_contacts", SHEET_NAME)

def get_data_feed_plan(access_token, data_source_id):
    if access_token == None:
        access_token = authenticate()

    # Only Activation should be CPM, the rest should be fixed
    data_feed_dict = {"SEGMENTS_AND_OVERLAP_PRICE":None,"MODELING_PRICE":None,"ACTIVATION_PRICE":None,
                    "SEGMENTS_AND_OVERLAP_UoM":None,"MODELING_UoM":None,"ACTIVATION_UoM":None}
    get_data_feed_plan_request = requests.get(DATA_FEED_PLAN_URL.format(data_source_id),
                        headers={
                            'Content-Type':"application/json",
                            'Authorization':"Bearer " + access_token
                        })
    print("Get Data Feed Plans URL: {}".format(get_data_feed_plan_request.url))
    variables.logger.warning("{} Get Data Feed Plans URL: {}".format(datetime.datetime.now().isoformat(), get_data_feed_plan_request.url))
    
    # No data feed for this data source
    if get_data_feed_plan_request.status_code == 404:
        return data_feed_dict

    data_feed_json = get_data_feed_plan_request.json()
    for data_feed in data_feed_json:
        useCase = data_feed["useCase"]
        billingUnit = data_feed["billingUnit"]
        price = data_feed["price"]

        # planId = data_feed["planId"]
        # dataSourceId = data_feed["dataSourceId"]
        # description = data_feed["description"]
        # status = data_feed["status"]
        # billingCycle = data_feed["billingCycle"]
        # createTime = data_feed["createTime"]
        # updateTime = data_feed["updateTime"]
        # crUID = data_feed["crUID"]
        # upUID = data_feed["upUID"]

        # with open("data_feed_plans.txt", "a+") as txtFile:
        #     txtFile.write(str(data_source_id)+"|"+str(useCase)+"|"+str(billingUnit)+"|"+str(price)+"|"+str(planId)+"|"+str(description)+"|"+str(status)+"|"+str(billingCycle)+"|"+str(createTime)+"|"+str(updateTime)+"|"+str(crUID)+"|"+str(upUID)+"\n")

        if len(useCase) == 1 and useCase[0] == "SEGMENTS_AND_OVERLAP":
            data_feed_dict["SEGMENTS_AND_OVERLAP_PRICE"] = price
            data_feed_dict["SEGMENTS_AND_OVERLAP_UoM"] = billingUnit
        elif len(useCase) == 1 and useCase[0] == "MODELING":
            data_feed_dict["MODELING_PRICE"] = price
            data_feed_dict["MODELING_UoM"] = billingUnit
        else:
            data_feed_dict["ACTIVATION_PRICE"] = price
            data_feed_dict["ACTIVATION_UoM"] = billingUnit

    return data_feed_dict

def get_all_data_source():
    access_token = authenticate()

    get_data_source_request = requests.get(DATA_SOURCE_URL,
                            headers={
                                'Content-Type':"application/json",
                                'Authorization':"Bearer " + access_token
                            })
    print("Get Data Source URL: {}".format(get_data_source_request.url))
    variables.logger.warning("{} Get Data Source URL: {}".format(datetime.datetime.now().isoformat(), get_data_source_request.url))

    data_source_json = get_data_source_request.json()
    return access_token, data_source_json

def get_data_source_id_dict():
    access_token, data_source_json = get_all_data_source()
    data_source_dict = {}

    # commented out all the unused parameters
    for data_source in data_source_json:
        # pid = data_source["pid"]
        name = data_source["name"]
        # description = data_source["description"]
        # status = data_source["status"]
        # integrationCode = data_source["integrationCode"]
        # dataExportRestrictions = data_source["dataExportRestrictions"]
        # updateTime = data_source["updateTime"]
        # crUID = data_source["crUID"]
        # upUID = data_source["upUID"]
        # data_source_type = data_source["type"]
        # inboundS2S = data_source["inboundS2S"]
        # outboundS2S = data_source["outboundS2S"]
        # useAudienceManagerVisitorID = data_source["useAudienceManagerVisitorID"]
        # allowDataSharing = data_source["allowDataSharing"]
        # masterDataSourceIdProvider = data_source["masterDataSourceIdProvider"]
        # idType = data_source["idType"]
        # allowDeviceGraphSharing = data_source["allowDeviceGraphSharing"]
        # supportsAuthenticatedProfile = data_source["supportsAuthenticatedProfile"]
        # deviceGraph = data_source["deviceGraph"]
        # authenticatedProfileName = data_source["authenticatedProfileName"]
        # deviceGraphName = data_source["deviceGraphName"]
        dataSourceId = data_source["dataSourceId"]
        # containerIds = data_source["containerIds"]
        # samplingEnabled = data_source["samplingEnabled"]

        # Get data feed for Data Source ID
        data_feed_dict = get_data_feed_plan(access_token, dataSourceId)
        segments_and_overlap_price = data_feed_dict["SEGMENTS_AND_OVERLAP_PRICE"]
        segments_and_overlap_uom = data_feed_dict["SEGMENTS_AND_OVERLAP_UoM"]
        modeling_price = data_feed_dict["MODELING_PRICE"]
        modeling_uom = data_feed_dict["MODELING_UoM"]
        activation_price = data_feed_dict["ACTIVATION_PRICE"]
        activation_uom = data_feed_dict["ACTIVATION_UoM"]

        data_source_dict[dataSourceId] = {"name":name, "segments_and_overlap_price":segments_and_overlap_price,"segments_and_overlap_uom":segments_and_overlap_uom,
                                            "modeling_price":modeling_price,"modeling_uom":modeling_uom,
                                            "activation_price":activation_price,"activation_uom":activation_uom}
    return access_token, data_source_dict

def get_data_feed_dict(access_token):
    data_feed_dict = {}

    if access_token == None:
        access_token = authenticate()

    get_data_feed_request = requests.get(DATA_FEED_URL,
                            headers={
                                'Content-Type':"application/json",
                                'Authorization':"Bearer " + access_token
                            })
    print("Get Data Feed URL: {}".format(get_data_feed_request.url))
    variables.logger.warning("{} Get Data Feed URL: {}".format(datetime.datetime.now().isoformat(), get_data_feed_request.url))

    data_feed_json = get_data_feed_request.json()

    for data_feed in data_feed_json:
        # name = data_feed["name"]
        description = data_feed["description"]
        # billing = data_feed["billing"]
        distribution = data_feed["distribution"]
        # status = data_feed["status"]
        # crUID = data_feed["crUID"]
        # upUID = data_feed["upUID"]
        # createTime = data_feed["createTime"]
        # updateTime = data_feed["updateTime"]
        # pid = data_feed["pid"]
        contactUserIds = data_feed["contactUserIds"]
        dataSourceId = data_feed["dataSourceId"]

        data_feed_dict[dataSourceId] = {
                                        "description":description,
                                        "distribution":distribution,
                                        "contactUserIds":contactUserIds
                                    }

    return data_feed_dict

# able to search the data source by name, returns Data Source ID
# Used for adding traits to existing data source
def get_data_source_name_dict():
    access_token, data_source_json = get_all_data_source()
    data_source_dict = {}

    # see get_data_source_id_dict for full list of fields available
    for data_source in data_source_json:
        # pid = data_source["pid"]
        name = data_source["name"]
        # so that when searching for the data source id, ignores case sensitivity
        lowercase_name = name.lower()
        dataSourceId = data_source["dataSourceId"]

        data_source_dict[lowercase_name] = dataSourceId
    return access_token, data_source_dict

def get_traits(access_token):
    if access_token == None:
        access_token = authenticate()

    get_trait_request = requests.get(TRAIT_URL,
                            params={
                                'includeDetails':True
                            },
                            headers={
                                'Authorization':"Bearer " + access_token
                            })
    print("Get Trait URL: {}".format(get_trait_request.url))
    variables.logger.warning("{} Get Trait URL: {}".format(datetime.datetime.now().isoformat(), get_trait_request.url))

    trait_json = get_trait_request.json()
    # print(trait_json)
    return access_token, trait_json

def add_trait(access_token, name, description, ttl, folderId, dataSourceId):
    if access_token == None:
        access_token = authenticate()

    add_trait_request = requests.post(TRAIT_URL,
                            headers={
                                'Content-Type':"application/json",
                                'Authorization':"Bearer " + access_token
                            },
                            json={
                                'backfillStatus':ADD_TRAIT_BACKFILL_STATUS,
                                'description':description,
                                'pid':PID,
                                'type':ADD_TRAIT_TYPE,
                                'folderId':folderId,
                                'ttl':ttl,
                                'dataSourceId':dataSourceId,
                                'traitType':ADD_TRAIT_TRAIT_TYPE,
                                'name':name,
                                'status':ADD_TRAIT_STATUS
                            }
                        )

    print("Add Trait URL: {}".format(add_trait_request.url))
    variables.logger.warning("{} Add Trait URL: {}".format(datetime.datetime.now().isoformat(), add_trait_request.url))

    add_trait_json = add_trait_request.json()
    # print(add_trait_json)

    response_status_code = add_trait_request.status_code
    if not response_status_code == 201:
        if 'childMessages' in add_trait_json:
            return access_token, response_status_code, add_trait_json['childMessages']
        else:
            return access_token, response_status_code, add_trait_json['message']
    
    return access_token, response_status_code, add_trait_json['sid']

def edit_trait_rule(access_token, sid, folderId, dataSourceId, name, description, ttl):
    if access_token == None:
        access_token = authenticate()

    edit_trait_request = requests.put(TRAIT_URL + str(sid),
                        headers={
                            'Content-Type':"application/json",
                            'Authorization':"Bearer " + access_token
                        },
                        json={
                            "backfillStatus":ADD_TRAIT_BACKFILL_STATUS,
                            "description":description,
                            'pid':PID,
                            'type':ADD_TRAIT_TYPE,
                            "folderId":folderId,
                            "ttl":ttl,
                            "dataSourceId":dataSourceId,
                            "traitType":ADD_TRAIT_TRAIT_TYPE,
                            "name":name,
                            "status":ADD_TRAIT_STATUS,
                            "traitRule":"ic==" + str(sid)
                        }
                    )
    print("Edit Trait URL: {}".format(edit_trait_request.url))
    variables.logger.warning("{} Edit Trait URL: {}".format(datetime.datetime.now().isoformat(), edit_trait_request.url))

    if not edit_trait_request.status_code == 200:
        print(edit_trait_request["message"])
        variables.logger.warning("{} ERROR: {}".format(datetime.datetime.now().isoformat(), edit_trait_request["message"]))
        return access_token, None

    edit_trait_json = edit_trait_request.json()
    
    return access_token, "Edited"

# Adding Data Source will add the name as the description as well
def add_data_source(access_token, data_source_name):
    if access_token == None:
        access_token = authenticate()

    add_data_source_request = requests.post(DATA_SOURCE_URL,
                        headers={
                            'Content-Type':"application/json",
                            'Authorization':"Bearer " + access_token
                        },
                        json={
                            "idType":DATA_SOURCE_ID_TYPE,
                            "outboundS2S":OUTBOUNDS2S,
                            "name":data_source_name,
                            "description":data_source_name,
                            "allowDataSharing":ALLOW_DATA_SHARING,
                            "inboundS2S":INBOUNDS2S,
                            "type":DATA_SOURCE_TYPE,
                            "dataExportRestrictions":DATA_EXPORT_RESTRICTIONS
                        })
    print("Add Data Source URL: {}".format(add_data_source_request.url))
    variables.logger.warning("{} Add Data Source URL: {}".format(datetime.datetime.now().isoformat(), add_data_source_request.url))

    add_data_source_status = add_data_source_request.status_code

    # Fail if status code is not 201
    if not add_data_source_status == 201:
        print(add_data_source_request.json())
        variables.logger.warning("{} ERROR: {}".format(datetime.datetime.now().isoformat(), add_data_source_request.json()))
        return access_token, None

    add_data_source_json = add_data_source_request.json()
    data_source_id = add_data_source_json["dataSourceId"]

    return access_token, data_source_id

def add_data_feed(access_token, data_source_id, data_source_name, data_feed_description, data_feed_distribution):
    if access_token == None:
        access_token = authenticate()

    if data_feed_distribution.lower() == "private":
        data_feed_distribution = "PRIVATE"
    else:
        data_feed_distribution = "PUBLIC"

    add_data_feed_request = requests.post(DATA_FEED_URL,
                                headers={
                                    'Content-Type':"application/json",
                                    'Authorization':"Bearer " + access_token
                                },
                                json={
                                    "dataBrandingType":ADD_DATA_FEED_DATA_BRANDING_TYPE,
                                    "dataSourceId":data_source_id,
                                    "name":data_source_name,
                                    "description":data_feed_description,
                                    "distribution":data_feed_distribution,
                                    "contactUserIds":ADD_DATA_FEED_CONTACT_USER_IDS,
                                    "billing":ADD_DATA_FEED_BILLING,
                                    "status":ADD_DATA_FEED_STATUS
                                }
                            )

    print("Add Data Feed URL: {}".format(add_data_feed_request.url))
    variables.logger.warning("{} Add Data Feed URL: {}".format(datetime.datetime.now().isoformat(), add_data_feed_request.url))
    add_data_feed_response = add_data_feed_request.json()

    if not add_data_feed_request.status_code == 201:
        return access_token, add_data_feed_response["message"]

    return access_token, "Created"

def add_data_feed_plan(access_token, dataSourceId, useCase, billingUnit, price):
    if access_token == None:
        access_token = authenticate()
    
    add_data_feed_plan_request = requests.post(DATA_FEED_PLAN_URL.format(dataSourceId),
                                    headers={
                                        'Content-Type':"application/json",
                                        'Authorization':"Bearer " + access_token
                                    },
                                    json={
                                        "useCase":useCase,
                                        "billingCycle":ADD_DATA_FEED_PLAN_BILLING_CYCLE,
                                        "price":price,
                                        "description":ADD_DATA_FEED_PLAN_DESCRIPTION,
                                        "billingUnit":billingUnit,
                                        "status":ADD_DATA_FEED_PLAN_STATUS
                                    }
                                )

    print("Add Data Feed Plan URL: {}".format(add_data_feed_plan_request.url))
    variables.logger.warning("{} Add Data Feed Plan URL: {}".format(datetime.datetime.now().isoformat(), add_data_feed_plan_request.url))
    add_data_feed_plan_response = add_data_feed_plan_request.json()

    if not add_data_feed_plan_request.status_code == 201:
        return access_token, "{} {}".format(add_data_feed_plan_response["message"],add_data_feed_plan_response["childMessages"])
    
    return access_token, "Created"

def get_trait_folders(access_token):
    if access_token == None:
        access_token = authenticate()
    
    get_trait_folder_request = requests.get(TRAIT_FOLDER_URL,
                                headers={
                                    'Content-Type':'application/json',
                                    'Authorization':"Bearer " + access_token
                                }
                            )
    print("Get Trait Folder URL: {}".format(get_trait_folder_request.url))
    variables.logger.warning("{} Get Trait Folder URL: {}".format(datetime.datetime.now().isoformat(), get_trait_folder_request.url))
    get_trait_folder_json = get_trait_folder_request.json()
    # print(get_trait_folder_json)

    if not get_trait_folder_request.status_code == 200:
        return None

    return access_token, get_trait_folder_json

# method to merge two dictionaries
def merge_dicts(dict_1, dict_2):
    temp_dict = dict_1.copy()
    temp_dict.update(dict_2)
    return temp_dict

# For query all function
def get_trait_folder_id_dict(access_token, trait_folder_json):
    trait_folder_dict = {}

    for trait_folder in trait_folder_json:
        folderId = trait_folder["folderId"]
        path = trait_folder["path"]

        if "subFolders" in trait_folder:
            subFolders = trait_folder["subFolders"]

            trait_subfolder_dict = get_trait_folder_id_dict(access_token, subFolders)
            trait_folder_dict = merge_dicts(trait_folder_dict, trait_subfolder_dict)
        
        trait_folder_dict[folderId] = path

    return trait_folder_dict

# For add segments function
def get_trait_folder_path_dict(access_token, trait_folder_json):
    trait_folder_dict = {}

    for trait_folder in trait_folder_json:
        folderId = trait_folder["folderId"]
        path = trait_folder["path"]

        if "subFolders" in trait_folder:
            subFolders = trait_folder["subFolders"]

            trait_subfolder_dict = get_trait_folder_path_dict(access_token, subFolders)
            trait_folder_dict = merge_dicts(trait_folder_dict, trait_subfolder_dict)
        
        trait_folder_dict[path] = folderId

    return trait_folder_dict

def add_trait_folder(access_token, parentFolderId, name):
    if access_token == None:
        access_token = authenticate()

    add_trait_folder_request = requests.post(TRAIT_FOLDER_URL,
                                headers={
                                    'Content-Type':'application/json',
                                    'Authorization':"Bearer " + access_token
                                },
                                json={
                                    "parentFolderId":parentFolderId,
                                    "name":name,
                                    "pid":PID
                                }
                            )
    print("Add Trait Folder URL: {}".format(add_trait_folder_request.url))
    variables.logger.warning("{} Add Trait Folder URL: {}".format(datetime.datetime.now().isoformat(), add_trait_folder_request.url))
    # print("access token: {}".format(access_token))
    # print("parentFolderId: {}".format(parentFolderId))
    # print("name: {}".format(name))
    # print("pid: {}".format(PID))
    add_trait_folder_response = add_trait_folder_request.json()
    # print(add_trait_folder_response)
    return access_token, add_trait_folder_response["folderId"]

def check_and_add_trait_folder(access_token, checked_path, trait_folder_path_list, trait_folder_name_dict, parent_folder_id):
    folder_id = None

    current_folder_name = trait_folder_path_list.pop(0)
    checked_path = checked_path + "/" + current_folder_name

    # This is not the childmost folder
    if len(trait_folder_path_list) > 0:
        # current path folder already exists
        if checked_path in trait_folder_name_dict:
            folder_id = trait_folder_name_dict[checked_path]
        # current path folder does not exist, create folder, append to dict, then look for next folder path
        else:
            access_token, folder_id = add_trait_folder(access_token, parent_folder_id, current_folder_name)
            trait_folder_name_dict[checked_path] = folder_id
        trait_folder_name_dict, folder_id = check_and_add_trait_folder(access_token, checked_path, trait_folder_path_list, trait_folder_name_dict, folder_id)

    else:
        if checked_path in trait_folder_name_dict:
            folder_id = trait_folder_name_dict[checked_path]
        else:
            access_token, folder_id = add_trait_folder(access_token, parent_folder_id, current_folder_name)
            trait_folder_name_dict[checked_path] = folder_id

    return trait_folder_name_dict, folder_id


# Process of getting all segments:
# 1. get all the data sources
# 2. for each data source ID, get the data feed plan (contains the price of segments_and_overlap, modeling, and activation)
# 3. get all the traits (these are your segments)
# 4. match them all together, you will get the segments to the data source, then to the data feeds to get the rates of each traits(segments)
def query_all_segments():
    segment_id_list = []
    segment_name_list = []
    segment_description_list = []
    segment_status_list = []
    segment_lifetime_list = []
    trait_folder_path_list = []
    data_source_id_list = []
    data_source_name_list = []
    data_feed_description_list = []
    data_feed_distribution_list = []
    data_feed_contactUserIds_list = []
    segments_and_overlap_price_list = []
    segments_and_overlap_uom_list = []
    modeling_price_list = []
    modeling_uom_list = []
    activation_price_list = []
    activation_uom_list = []

    access_token, data_source_dict = get_data_source_id_dict()
    data_feed_dict = get_data_feed_dict(access_token)
    access_token, trait_json = get_traits(None)

    # Get Trait Folder
    access_token, trait_folder_json = get_trait_folders(access_token)
    trait_folder_id_dict = get_trait_folder_id_dict(access_token, trait_folder_json)

    for trait in trait_json:
        # createTime = trait["createTime"]
        # updateTime = trait["updateTime"]
        sid = trait["sid"]
        # traitType = trait["traitType"]
        name = trait["name"]
        description = trait["description"]
        status = None
        if "status" in trait:
            status = trait["status"]
        # pid = trait["pid"]
        # crUID = trait["crUID"]
        # upUID = trait["upUID"]
        ttl = None
        if 'ttl' in trait:
            ttl = trait['ttl']
        # integrationCode = trait["integrationCode"]
        # traitRule = None
        # if 'traitRule' in item:
        #     traitRule = item['traitRule']
        # traitRuleVersion = None
        # if 'traitRuleVersion' in item:
        #     traitRuleVersion = item['traitRuleVersion']
        # trait_type = None
        # if 'type' in item:
        #     trait_type = item['type']
        # backfillStatus = None
        # if 'backfillStatus' in item:
        #     backfillStatus = item['backfillStatus']
        dataSourceId = trait["dataSourceId"]
        folderId = trait["folderId"]

        data_source = data_source_dict[dataSourceId]
        data_source_name = data_source["name"]

        # some data source might not have data feed
        data_feed_description = None
        data_feed_distribution = None
        data_feed_contactUserIds = None
        if dataSourceId in data_feed_dict:
            data_feed = data_feed_dict[dataSourceId]
            data_feed_description = data_feed["description"]
            data_feed_distribution = data_feed["distribution"]
            data_feed_contactUserIds = data_feed["contactUserIds"]
        
        segments_and_overlap_price = data_source["segments_and_overlap_price"]
        segments_and_overlap_uom = data_source["segments_and_overlap_uom"]
        modeling_price = data_source["modeling_price"]
        modeling_uom = data_source["modeling_uom"]
        activation_price = data_source["activation_price"]
        activation_uom = data_source["activation_uom"]
        trait_folder_path = trait_folder_id_dict[folderId]

        segment_id_list.append(sid)
        segment_name_list.append(name)
        segment_description_list.append(description)
        segment_status_list.append(status)
        segment_lifetime_list.append(ttl)
        trait_folder_path_list.append(trait_folder_path)
        data_source_id_list.append(dataSourceId)
        data_source_name_list.append(data_source_name)
        data_feed_description_list.append(data_feed_description)
        data_feed_distribution_list.append(data_feed_distribution)
        data_feed_contactUserIds_list.append(data_feed_contactUserIds)
        segments_and_overlap_price_list.append(segments_and_overlap_price)
        segments_and_overlap_uom_list.append(segments_and_overlap_uom)
        modeling_price_list.append(modeling_price)
        modeling_uom_list.append(modeling_uom)
        activation_price_list.append(activation_price)
        activation_uom_list.append(activation_uom)

    write_df = pd.DataFrame({
                    "Segment ID":segment_id_list,
                    "Segment Name":segment_name_list,
                    "Segment Description":segment_description_list,
                    "Segment Lifetime":segment_lifetime_list,
                    "Trait Folder Path":trait_folder_path_list,
                    "Data Source ID":data_source_id_list,
                    "Data Source Name":data_source_name_list,
                    "Data Feed Description":data_feed_description_list,
                    "Distribution":data_feed_distribution_list,
                    "Contact Users":data_feed_contactUserIds_list,
                    "Segments and Overlap Price":segments_and_overlap_price_list,
                    "Segments and Overlap UoM":segments_and_overlap_uom_list,
                    "Modeling Price":modeling_price_list,
                    "Modeling UoM":modeling_uom_list,
                    "Activation Price":activation_price_list,
                    "Activation UoM":activation_uom_list,
                    "Segment Status":segment_status_list,
                })
    return write_excel.write_and_email(write_df, "DONOTUPLOAD_AdobeAAM_query_all", SHEET_NAME)
    # return write_excel.write(write_df, "DONOTUPLOAD_AdobeAAM_query_all", SHEET_NAME)

# Process to add segments:
# 1. Check if data source exists, if not, create data source
# 2. if data source is created, create data feed
# 3. if data feed is created, create data feed plan
# 4. Check if trait folder exists, if not, create trait folder
# 5. create trait
def read_all_to_add_segments(file_path):
    add_segments_start_time = time.time()
    add_segments_authenticate_count = 1

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}
    
    segment_id_list = []
    segment_name_list = read_df["Segment Name"]
    segment_description_list = read_df["Segment Description"]
    segment_status_list = []
    segment_lifetime_list = read_df["Segment Lifetime"]
    trait_folder_path_list = read_df["Trait Folder Path"]
    data_source_id_list = []
    data_source_name_list = read_df["Data Source Name"]
    data_feed_description_list = read_df["Data Feed Description"]
    data_feed_distribution_list = read_df["Distribution"]
    segments_and_overlap_price_list = read_df["Segments and Overlap Price"]
    segments_and_overlap_uom_list = []
    modeling_price_list = read_df["Modeling Price"]
    modeling_uom_list = read_df["Modeling UoM"]
    activation_price_list = read_df["Activation Price"]
    activation_uom_list = read_df["Activation UoM"]
    eyeota_buyer_id_list = []
    data_source_result = []
    data_feed_result = []
    segments_and_overlap_plan_result = []
    modeling_plan_result = []
    activation_plan_result = []
    create_trait_folder_result = []
    create_trait_result = []

    access_token, data_source_name_dict = get_data_source_name_dict()

    row_counter = 0
    for data_source_name in data_source_name_list:
        lowercase_data_source_name = data_source_name.lower()
        
        data_source_id = None
        if lowercase_data_source_name in data_source_name_dict:
            data_source_id = data_source_name_dict[lowercase_data_source_name]
            data_source_result.append("Existing Data Source")
            data_feed_result.append(None)
            segments_and_overlap_plan_result.append(None)
            modeling_plan_result.append(None)
            activation_plan_result.append(None)
        # if name not in dict, add data source, append data source to dict (lowercase name: data source id)
        else:
            # Add case sensitive name for data source
            access_token, data_source_id = add_data_source(None, data_source_name)

            # Fail to add new data source
            if data_source_id == None:
                data_source_result.append("FAILED")
                segment_status_list.append(None)
                data_feed_result.append(None)
                segments_and_overlap_plan_result.append(None)
                modeling_plan_result.append(None)
                activation_plan_result.append(None)
                create_trait_folder_result.append(None)
                segment_id_list.append(None)
                eyeota_buyer_id_list.append(None)
            else:
                # Append lower case name to data source name dict
                data_source_name_dict[lowercase_data_source_name] = data_source_id
                data_source_result.append("Created")

                data_source_name = data_source_name_list[row_counter]
                data_feed_description = data_feed_description_list[row_counter]
                data_feed_distribution = data_feed_distribution_list[row_counter]
                access_token, add_data_feed_output = add_data_feed(access_token, data_source_id, data_source_name, data_feed_description, data_feed_distribution)

                if not add_data_feed_output == "Created":
                    data_feed_result.append("Failed. " + add_data_feed_output)
                    segments_and_overlap_plan_result.append(None)
                    modeling_plan_result.append(None)
                    activation_plan_result.append(None)
                    create_trait_folder_result.append(None)
                    segment_id_list.append(None)
                else:
                    data_feed_result.append("Created")
                    
                    # Segments and overlap must be created
                    segments_and_overlap_price = segments_and_overlap_price_list[row_counter]
                    # convert segments_and_overlap_price to float
                    try:
                        segments_and_overlap_price = float(segments_and_overlap_price)
                        access_token, segments_and_overlap_output = add_data_feed_plan(access_token, data_source_id, ["SEGMENTS_AND_OVERLAP"], ADD_DATA_FEED_PLAN_SEGMENT_AND_OVERLAP_BILLING_UNIT, segments_and_overlap_price)
                        segments_and_overlap_plan_result.append(segments_and_overlap_output)
                        create_data_source_success = True
                    except:
                        segments_and_overlap_plan_result.append("FAILED. Please enter a number for Segments and Overlap Price")
                        create_trait_folder_result.append(None)
                        segment_id_list.append(None)

                    # Modeling will be created if modeling price is not empty
                    modeling_billing_unit = modeling_uom_list[row_counter]

                    if not pd.isna(modeling_billing_unit):
                        modeling_price = modeling_price_list[row_counter]
                        
                        if modeling_billing_unit == "FIXED" or modeling_billing_unit == "CPM":
                            # convert modeling_price to float
                            try:
                                modeling_price = float(modeling_price)
                                access_token, modeling_plan_output = add_data_feed_plan(access_token, data_source_id, ["MODELING"], modeling_billing_unit, modeling_price)
                                modeling_plan_result.append(modeling_plan_output)
                            except:
                                modeling_plan_result.append("FAILED. Please enter a number for Modeling Price")
                        else:
                            modeling_plan_result.append("Failed. Only enter FIXED or CPM for Modeling UoM")
                    else:
                        modeling_plan_result.append(None)
                        

                    # Activation will be created if activation price is not empty (what billing unit to add?)
                    activation_billing_unit = activation_uom_list[row_counter]

                    if not pd.isna(activation_billing_unit):
                        activation_price = activation_price_list[row_counter]
                        
                        if activation_billing_unit == "FIXED" or activation_billing_unit == "CPM":
                            # convert activation_price to float
                            try:
                                activation_price = float(activation_price)
                                access_token, activation_plan_output = add_data_feed_plan(access_token, data_source_id, ['AD_TARGETING', 'PERSONALIZATION_AND_TESTING', 'FEED_EXPORT'], activation_billing_unit, activation_price)
                                activation_plan_result.append(activation_plan_output)
                            except:
                                activation_plan_result.append("FAILED. Please enter a number for Activation Price")
                        else:
                            activation_plan_result.append("Failed. Only enter FIXED or CPM for Activation UoM")
                    else:
                        activation_plan_result.append(None)
            
        folder_id = None
        # Create trait folder if data source exists

        access_token, trait_folder_json = get_trait_folders(access_token)
        trait_folder_path_dict = get_trait_folder_path_dict(access_token, trait_folder_json)

        if not data_source_id == None:
            trait_folder_path = trait_folder_path_list[row_counter]

            if trait_folder_path in trait_folder_path_dict:
                folder_id = trait_folder_path_dict[trait_folder_path]
                create_trait_folder_result.append("Existing folder")
            else:
                # remove first slash from the path
                trait_folder_path = trait_folder_path[1:]
                trait_folder_path_split = trait_folder_path.split("/")
                trait_folder_path_dict, folder_id = check_and_add_trait_folder(access_token, "", trait_folder_path_split, trait_folder_path_dict, 0)

                create_trait_folder_result.append("Created")
        
        # Create trait if data source and trait folder exists
        create_trait_output = None

        if not data_source_id == None and not folder_id == None:
            segment_name = segment_name_list[row_counter]
            segment_description = segment_description_list[row_counter]
            if pd.isna(segment_description):
                segment_description = ""
            segment_lifetime = segment_lifetime_list[row_counter]

            try:
                segment_lifetime = int(segment_lifetime)
            except:
                segment_id_list.append(None)
                segment_status_list.append(None)
                create_trait_output = "Failed. Please enter a number for Segment Lifetime"
                eyeota_buyer_id_list.append(None)

            # segment lifetime is numerical
            if create_trait_output == None:
                access_token, status_code, output = add_trait(access_token, segment_name, segment_description, segment_lifetime, folder_id, data_source_id)

                if not status_code == 201:
                    segment_id_list.append(None)
                    segment_status_list.append(None)
                    create_trait_output = output
                    eyeota_buyer_id_list.append(None)
                else:
                    segment_id_list.append(output)
                    segment_status_list.append(ADD_TRAIT_STATUS)
                    access_token, edit_trait_result = edit_trait_rule(access_token, output, folder_id, data_source_id, segment_name, segment_description, segment_lifetime)

                    if edit_trait_result == None:
                        create_trait_output = "Failed to edit Trait Expression"
                        eyeota_buyer_id_list.append(None)
                    else:
                        create_trait_output = "Created"
                        eyeota_buyer_id_list.append("{}:{}".format(data_source_id, output))

        create_trait_result.append(create_trait_output)

        row_counter += 1
        data_source_id_list.append(data_source_id)

        # Get access token again if process > authenticate time limit
        add_segments_current_time = time.time()
        add_segments_elapsed_secs = add_segments_current_time - add_segments_start_time
        add_segments_authentication_timeover = add_segments_elapsed_secs - AUTHENTICATION_LIMIT_SECS * add_segments_authenticate_count

        if add_segments_authentication_timeover > 0:
            print("Authenticating...")
            variables.logger.warning("{} Authenticating...".format(datetime.datetime.now().isoformat()))
            access_token = authenticate()
            add_segments_authenticate_count += 1

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    # print("Segment Name: {}".format(len(segment_name_list)))
    # print("Segment Description: {}".format(len(segment_description_list)))
    # print("Segment Status: {}".format(len(segment_status_list)))
    # print("Segment Lifetime: {}".format(len(segment_lifetime_list)))
    # print("Trait Folder Path: {}".format(len(trait_folder_path_list)))
    # print("Data Source ID: {}".format(len(data_source_id_list)))
    # print("Data Source Name: {}".format(len(data_source_name_list)))
    # print("Data Feed Description: {}".format(len(data_feed_description_list)))
    # print("Distribution: {}".format(len(data_feed_distribution_list)))
    # print("Data Source Result: {}".format(len(data_source_result)))
    # print("Data Feed Result: {}".format(len(data_feed_result)))
    # print("Segments and Overlap Plan Result: {}".format(len(segments_and_overlap_plan_result)))
    # print("Modeling Plan Result: {}".format(len(modeling_plan_result)))
    # print("Activation Plan Result: {}".format(len(activation_plan_result)))
    # print("Trait Folder Result: {}".format(len(create_trait_folder_result)))
    # print("Create Segment Result: {}".format(len(create_trait_result)))
    # print("Eyeota Buyer ID: {}".format(len(eyeota_buyer_id_list)))

    write_df = pd.DataFrame({
                    "Segment Name":segment_name_list,
                    "Segment Description":segment_description_list,
                    "Segment Status":segment_status_list,
                    "Segment Lifetime":segment_lifetime_list,
                    "Trait Folder Path":trait_folder_path_list,
                    "Data Source ID":data_source_id_list,
                    "Data Source Name":data_source_name_list,
                    "Data Feed Description":data_feed_description_list,
                    "Distribution":data_feed_distribution_list,
                    "Data Source Result":data_source_result,
                    "Data Feed Result":data_feed_result,
                    "Segments and Overlap Plan Result":segments_and_overlap_plan_result,
                    "Modeling Plan Result":modeling_plan_result,
                    "Activation Plan Result":activation_plan_result,
                    "Trait Folder Result":create_trait_folder_result,
                    "Create Segment Result": create_trait_result,
                    "Eyeota Buyer ID": eyeota_buyer_id_list
                })
    return write_excel.write(write_df, file_name + "_output_add_segments", SHEET_NAME)

def read_file_to_edit_segments(file_path):
    edit_segments_start_time = time.time()
    edit_segments_authenticate_count = 1

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}
    
    segment_id_list = read_df["Segment ID"]
    segment_name_list = read_df["Segment Name"]
    segment_description_list = read_df["Segment Description"]
    segment_lifetime_list = read_df["Segment Lifetime"]
    trait_folder_path_list = read_df["Trait Folder Path"]
    data_source_name_list = read_df["Data Source Name"]
    edit_trait_result = []

    access_token, data_source_name_dict = get_data_source_name_dict()
    access_token, trait_folder_json = get_trait_folders(access_token)
    trait_folder_path_dict = get_trait_folder_path_dict(access_token, trait_folder_json)

    row_counter = 0
    for segment_name in segment_name_list:
        trait_folder_path = trait_folder_path_list[row_counter]
        data_source_name = data_source_name_list[row_counter]
        segment_description = segment_description_list[row_counter]
        segment_id = segment_id_list[row_counter]
        segment_lifetime = segment_lifetime_list[row_counter]
        numerical_lifetime = True
        edit_output = ""

        data_source_name_lower = data_source_name.lower()
        data_source_id = -1
        try:
            data_source_id = data_source_name_dict[data_source_name_lower]
        except:
            print("Data Source ID not found for '{}'. ".format(data_source_name))
            variables.logger.warning("{} ERROR: {}".format(datetime.datetime.now().isoformat(), "Data Source ID not found for '{}'. ".format(data_source_name)))
            edit_output = edit_output + "Data Source ID not found for '{}'. ".format(data_source_name)

        folder_id = -1
        try:
            folder_id = trait_folder_path_dict[trait_folder_path]
        except:
            print("Folder ID not found for '{}'. ".format(trait_folder_path))
            variables.logger.warning("{} ERROR: {}".format(datetime.datetime.now().isoformat(), "Folder ID not found for '{}'. ".format(trait_folder_path)))
            edit_output = edit_output + "Folder ID not found for '{}'. ".format(trait_folder_path)

        try:
            segment_lifetime = int(segment_lifetime)
        except:
            numerical_lifetime = False
            print("Segment Lifetime '{}' is not numerical".format(segment_lifetime))
            variables.logger.warning("{} ERROR: {}".format(datetime.datetime.now().isoformat(), "Segment Lifetime '{}' is not numerical".format(segment_lifetime)))
            edit_output = edit_output + "Segment Lifetime '{}' is not numerical".format(segment_lifetime)

        if not data_source_id == -1 and not folder_id == -1 and numerical_lifetime:
            access_token, edit_output = edit_trait_rule(access_token, segment_id, folder_id, data_source_id, segment_name, segment_description, segment_lifetime)
        
        edit_trait_result.append(edit_output)

        # Get access token again if process > authenticate time limit
        edit_segments_current_time = time.time()
        edit_segments_elapsed_secs = edit_segments_current_time - edit_segments_start_time
        edit_segments_authentication_timeover = edit_segments_elapsed_secs - AUTHENTICATION_LIMIT_SECS * edit_segments_authenticate_count

        if edit_segments_authentication_timeover > 0:
            print("Authenticating...")
            variables.logger.warning("{} Authenticating...".format(datetime.datetime.now().isoformat()))
            access_token = authenticate()
            edit_segments_authenticate_count += 1

        row_counter += 1

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    write_df = pd.DataFrame({
        "Segment ID":segment_id_list,
        "Segment Name":segment_name_list,
        "Segment Lifetime":segment_lifetime_list,
        "Trait Folder Path":trait_folder_path_list,
        "Data Source Name":data_source_name_list,
        "Edit Output":edit_trait_result
    })
    return write_excel.write(write_df, file_name + "_output_edit_segments", SHEET_NAME)

def get_trait_uniques(access_token, dataSourceId):
    if access_token == None:
        access_token = authenticate()

    get_trait_request = requests.get(TRAIT_URL,
                            params={
                                'includeDetails':True,
                                'includeMetrics':True,
                                'dataSourceId':dataSourceId
                            },
                            headers={
                                'Authorization':"Bearer " + access_token
                            })
    print("Get Trait URL: {}".format(get_trait_request.url))
    variables.logger.warning("{} Get Trait URL: {}".format(datetime.datetime.now().isoformat(), get_trait_request.url))

    return access_token, get_trait_request.json()

def read_all_to_get_uniques_report(file_path):
    segment_id_list = []
    segment_name_list = []
    segment_description_list = []
    segment_status_list = []
    segment_lifetime_list = []
    data_source_id_list = []
    data_source_name_list = []
    uniques_1_day_list = []
    uniques_7_day_list = []
    uniques_14_day_list = []
    uniques_30_day_list = []
    uniques_60_day_list = []
    # uniques_90_day_list = []

    access_token, data_source_dict = get_data_source_name_dict()

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    given_data_source_name_list = read_df["Data Source Name"]
    
    for given_data_source_name in given_data_source_name_list:
        given_data_source_id = data_source_dict[given_data_source_name.lower()]
        access_token, trait_json = get_trait_uniques(access_token, given_data_source_id)

        for trait in trait_json:
            dataSourceId = trait["dataSourceId"]

            sid = trait["sid"]
            name = trait["name"]
            description = trait["description"]
            status = None
            if "status" in trait:
                status = trait["status"]
            
            ttl = None
            if 'ttl' in trait:
                ttl = trait['ttl']

            uniques_1_day = trait["uniques1Day"]
            uniques_7_day = trait["uniques7Day"]
            uniques_14_day = trait["uniques14Day"]
            uniques_30_day = trait["uniques30Day"]
            uniques_60_day = trait["uniques60Day"]
            # uniques_90_day = trait["uniques90Day"]

            segment_id_list.append(sid)
            segment_name_list.append(name)
            segment_description_list.append(description)
            segment_status_list.append(status)
            segment_lifetime_list.append(ttl)
            data_source_id_list.append(dataSourceId)
            data_source_name_list.append(given_data_source_name)
            uniques_1_day_list.append(uniques_1_day)
            uniques_7_day_list.append(uniques_7_day)
            uniques_14_day_list.append(uniques_14_day)
            uniques_30_day_list.append(uniques_30_day)
            uniques_60_day_list.append(uniques_60_day)
            # uniques_90_day_list.append(uniques_90_day)

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    write_df = pd.DataFrame({
        "Segment ID":segment_id_list,
        "Segment Name":segment_name_list,
        "Segment Description":segment_description_list,
        "Status": segment_status_list,
        "Segment Lifetime":segment_lifetime_list,
        "Data Source ID": data_source_id_list,
        "Data Source Name":data_source_name_list,
        "Uniques 1 Day":uniques_1_day_list,
        "Uniques 7 Day":uniques_7_day_list,
        "Uniques 14 Day":uniques_14_day_list,
        "Uniques 30 Day":uniques_30_day_list,
        "Uniques 60 Day":uniques_60_day_list
        # "Uniques 90 Day":uniques_90_day_list
    })
    return write_excel.write_and_email(write_df, file_name + "_output_get_uniques", SHEET_NAME)

def read_all_to_get_trait_rule(file_path):
    write_segment_id_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_lifetime_list = []
    write_data_source_id_list = []
    write_trait_rule_list = []

    access_token = authenticate()

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    given_segment_id_list = read_df["Segment ID"]

    for given_segment_id in given_segment_id_list:
        segment_id, segment_name, segment_description, ttl, data_source_id, trait_rule = get_trait_rule(access_token, given_segment_id)

        write_segment_id_list.append(segment_id)
        write_segment_name_list.append(segment_name)
        write_segment_description_list.append(segment_description)
        write_lifetime_list.append(ttl)
        write_data_source_id_list.append(data_source_id)
        write_trait_rule_list.append(trait_rule)

    write_df = pd.DataFrame({
        "Segment ID":write_segment_id_list,
        "Segment Name":write_segment_name_list,
        "Segment Description":write_segment_description_list,
        "Lifetime":write_lifetime_list,
        "Data Source ID":write_data_source_id_list,
        "Trait Rule":write_trait_rule_list
    })

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    return write_excel.write_and_email(write_df, file_name + "_output_get_trait_rule", SHEET_NAME)

def get_trait_rule(access_token, segment_id):
    if access_token == None:
        access_token = authenticate()
    
    get_trait_rule_request = requests.get(TRAIT_RULE_URL.format(segment_id),
                                    headers={
                                        'Content-Type':"application/json",
                                        'Authorization':"Bearer " + access_token
                                    }
                                )

    print("Get Trait Rule URL: {}".format(get_trait_rule_request.url))
    variables.logger.warning("{} Get Trait Rule URL: {}".format(datetime.datetime.now().isoformat(), get_trait_rule_request.url))
    get_trait_rule_response = get_trait_rule_request.json()

    if not get_trait_rule_request.status_code == 200:
        return "{} Error for segment id: {}".format(get_trait_rule_request.status_code, segment_id), None, None, None, None, None
    
    try:
        sid = get_trait_rule_response["sid"]
        segment_name = get_trait_rule_response["name"]
        segment_description = get_trait_rule_response["description"]
        lifetime = get_trait_rule_response["ttl"]
        data_source_id = get_trait_rule_response["dataSourceId"]
        trait_rule = get_trait_rule_response["traitRule"]

        return sid, segment_name, segment_description, lifetime, data_source_id, trait_rule
    except:
        return "Error for segment id: {}".format(segment_id), None, None, None, None, None