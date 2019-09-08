import requests
import variables
import write_excel
import pandas as pd
import datetime
import json
import os
import time

AUTH_URL = "https://dmp-api.adform.com/v1/token"
API_URL = "https://api.adform.com/v1/dmp/"

SHEET_NAME = "Adform"

# Login credentials
username = None
password = None
CONTENT_TYPE = "application/x-www-form-urlencoded"
GRANT_TYPE = "password"

# Query all constants
CATEGORIES_URL = API_URL + "categories"
SEGMENTS_URL = API_URL + "segments"
LIMIT = 5000

# Add all constants
SEGMENT_STATUS = "active"

# Data Provider Name
DATA_PROVIDER_URL = API_URL + "dataproviders/"

# Report all constants
REPORTS_URL = API_URL + "reports/"
DATA_USAGE_REPORT_URL = REPORTS_URL + "datausage"
AUDIENCE_REPORT_URL = REPORTS_URL + "audience"

def callAPI(function, file_path):
    start_time = time.time()
    output = {"message":"ERROR: option is not available"}

    if function == "Add Segments":
        output = read_file_to_add_segments(file_path)
    elif function == "Edit Segments":
        output = read_file_to_edit_segments(file_path)
    elif function == "Delete Segments":
        output = read_file_to_delete_segments(file_path)
    elif function == "Query All Segments":
        output = get_all_segments()
    elif function == "Data Usage Report":
        file_names = read_file_to_get_report(file_path, SHEET_NAME, "data_usage")
        output = write_excel.return_report(file_names, SHEET_NAME, file_path)
    elif function == "Audience Report":
        file_names = read_file_to_get_report(file_path, SHEET_NAME, "audience")
        output = write_excel.return_report(file_names, SHEET_NAME, file_path)

    elapsed_time = time.time() - start_time
    elapsed_mins = int(elapsed_time/60)
    elapsed_secs = int(elapsed_time%60)

    print("Elapsed time: {} mins {} secs".format(elapsed_mins, elapsed_secs))
    return output

def authenticate():
    try:
        global username; username = variables.login_credentials['Adform']['Login']
        global password; password = variables.login_credentials['Adform']['PW']
    except:
        return {"message":"ERROR: Incorrect login credentials! Please download 'asoh-flask-deploy.sh' file from <a href='https://eyeota.atlassian.net/wiki/pages/viewpageattachments.action?pageId=127336529&metadataLink=true'>Confluence</a> again!>"}

    data = {
                "grant_type":GRANT_TYPE,
                "username":username,
                "password":password
            }
    auth_request = requests.post(AUTH_URL,
                            headers={
                                'Content-Type':CONTENT_TYPE
                            },
                            data=data)
    print("Authenticate URL: {}".format(auth_request.url))
    variables.logger.warning("{} Authenticate URL: {}".format(datetime.datetime.now().isoformat(), auth_request.url))
    auth_json = auth_request.json()
    auth_token = auth_json["access_token"] 
    return auth_token

def get_categories(access_token):
    get_categories_request = requests.get(CATEGORIES_URL,
                                        headers={
                                            'Content-Type':'application/json',
                                            'Authorization':'Bearer ' + access_token
                                        })
    print("Get Categories URL: {}".format(get_categories_request.url))
    variables.logger.warning("{} Get Categories URL: {}".format(datetime.datetime.now().isoformat(), get_categories_request.url))
    if not get_categories_request.status_code == 200:
        return None

    return get_categories_request.json()

def store_category_in_dict(categories_json):
    categories_dict = {}

    for category in categories_json:
        category_id = category["id"]
        category_name = category["name"]
        category_dataProviderId = category["dataProviderId"]
        category_parentId = None
        if "parentId" in category:
            category_parentId = category["parentId"]
        category_regions = category["regions"]
        category_updatedAt = category["updatedAt"]
        category_createdAt = category["createdAt"]

        categories_dict[category_id] = {"name":category_name,
                                        "dataProviderId":category_dataProviderId,
                                        "parentId":category_parentId,
                                        "regions":category_regions,
                                        "updatedAt":category_updatedAt,
                                        "createdAt":category_createdAt}
    
    return categories_dict

def get_full_category_name(parent_category_id, child_category_name, categories_dict):
    while parent_category_id in categories_dict and not parent_category_id == None:
        child_category_name = categories_dict[parent_category_id]["name"] + " - " + child_category_name
        parent_category_id = categories_dict[parent_category_id]["parentId"]
    return child_category_name

def get_segments(access_token, offset):
    get_segments_request = requests.get(SEGMENTS_URL,
                                        headers={
                                            'Content-Type':'application/json',
                                            'Authorization':'Bearer ' + access_token
                                        },
                                        params={
                                            'returnTotalCount':True,
                                            'offset':offset,
                                            'limit':LIMIT
                                        })
    print("Get Segments URL: {}".format(get_segments_request.url))
    variables.logger.warning("{} Get Segments URL: {}".format(datetime.datetime.now().isoformat(), get_segments_request.url))
    if not get_segments_request.status_code == 200:
        return None

    headers_dict = dict(get_segments_request.headers)
    total_count = int(headers_dict["Total-Count"])
    return total_count, get_segments_request.json()

def get_all_segments():
    segment_id_list = []
    segment_dataProviderName_list = []
    segment_status_list = []
    segment_categoryId_list = []
    segment_refId_list = []
    segment_fee_list = []
    segment_ttl_list = []
    segment_name_list = []
    segment_audience_list = []
    segment_clicks_list = []
    segment_impressions_list = []

    data_provider_list = {}

    access_token = authenticate()
    if "message" in access_token:
        return access_token

    categories_json = get_categories(access_token)

    if categories_json == None:
        return {"message":"ERROR: unable to retrieve categories"}
    
    categories_dict = store_category_in_dict(categories_json)

    segment_offset = 0
    segment_total = LIMIT

    while segment_offset < segment_total:
        segment_total, segments_json = get_segments(access_token, segment_offset)

        for segment in segments_json:
            segment_id = segment["id"]
            segment_dataProviderId = segment["dataProviderId"]

            data_provider_name = None
            if segment_dataProviderId in data_provider_list:
                data_provider_name = data_provider_list[segment_dataProviderId]
            else:
                data_provider_name = get_data_provider_name(access_token, int(segment_dataProviderId))
                data_provider_list[segment_dataProviderId] = data_provider_name

            segment_status = segment["status"]
            segment_categoryId = segment["categoryId"]
            segment_refId = segment["refId"]
            segment_fee = segment["fee"]
            segment_ttl = segment["ttl"]
            segment_name = segment["name"]
            segment_audience = segment["audience"]

            segment_usageStatistics = None
            segment_clicks = None
            segment_impressions = None
            segment_rank = None
            if "usageStatistics" in segment:
                segment_usageStatistics = segment["usageStatistics"]
                segment_clicks = segment_usageStatistics["clicks"]
                segment_impressions = segment_usageStatistics["impressions"]

            category_parentId = categories_dict[segment_categoryId]["parentId"]
            category_name = categories_dict[segment_categoryId]["name"]
            category_full_name = get_full_category_name(category_parentId, category_name, categories_dict)

            segment_full_name = category_full_name + " - " + segment_name

            segment_id_list.append(segment_id)
            segment_dataProviderName_list.append(data_provider_name)
            segment_status_list.append(segment_status)
            segment_categoryId_list.append(segment_categoryId)
            segment_refId_list.append(segment_refId)
            segment_fee_list.append(segment_fee)
            segment_ttl_list.append(segment_ttl)
            segment_name_list.append(segment_full_name)
            segment_audience_list.append(segment_audience)
            segment_clicks_list.append(segment_clicks)
            segment_impressions_list.append(segment_impressions)

        segment_offset += LIMIT

    write_df = pd.DataFrame({
        "Segment ID":segment_id_list,
        "Ref ID":segment_refId_list,
        "Segment Name":segment_name_list,
        "Status":segment_status_list,
        "Fee":segment_fee_list,
        "TTL":segment_ttl_list,
        "Audience":segment_audience_list,
        "Clicks":segment_clicks_list,
        "Impressions":segment_impressions_list,
        "Data Provider Name":segment_dataProviderName_list,
        "Category ID":segment_categoryId_list
    })
    return write_excel.write_and_email(write_df, "DONOTUPLOAD_Adform_query_all", SHEET_NAME)
    # return write_excel.write(write_df, "DONOTUPLOAD_Adform_query_all", SHEET_NAME)

# Add segment functions
def add_category(access_token, category_name, data_provider_id, region, parent_category_id):
    add_category_json = None
    if parent_category_id == None:
        add_category_json = {
                                "Name": category_name,
                                "DataProviderId": data_provider_id,
                                "Regions": []
                            }
    else:
        add_category_json = {
                                "Name": category_name,
                                "DataProviderId": data_provider_id,
                                "ParentId": parent_category_id,
                                "Regions": []
                            }

    add_category_request = requests.post(CATEGORIES_URL,
                            headers={
                                'Content-Type':'application/json',
                                'Authorization':'Bearer ' + access_token
                            },
                            json=add_category_json)
    print("Add Category URL: {}".format(add_category_request.url))
    variables.logger.warning("{} Add Category URL: {}".format(datetime.datetime.now().isoformat(), add_category_request.url))
    # print(add_category_request.json())
    if add_category_request.status_code == 201:
        add_category_response = add_category_request.json()
        return add_category_response["id"]
    else:
        return None

def add_segment(access_token, data_provider_id, region, category_id, ref_id, fee, ttl, name, status):
    add_segment_request = requests.post(SEGMENTS_URL,
                            headers={
                                "Content-Type":"application/json",
                                "Authorization":"Bearer " + access_token
                            },
                            json={
                                "DataProviderId":data_provider_id,
                                "Status":status,
                                "CategoryId":category_id,
                                "RefId":str(ref_id),
                                "Fee":fee,
                                "Ttl":ttl,
                                "Name":name,
                                "Frequency":1
                            })
    print("Add Segment URL: {}".format(add_segment_request.url))
    variables.logger.warning("{} Add Segment URL: {}".format(datetime.datetime.now().isoformat(), add_segment_request.url))

    add_segment_json = add_segment_request.json()
    # print(add_segment_json)
    if add_segment_request.status_code == 201:
        segment_id = add_segment_json["id"]
        return 201, segment_id
    else:
        return None, add_segment_json["params"]

def delete_segment(access_token, segment_id):
    delete_segment_request = requests.delete(SEGMENTS_URL + "/" + str(segment_id),
                            headers={
                                "Authorization":"Bearer " + access_token
                            })
    print("Delete Segment URL: {}".format(delete_segment_request.url))
    variables.logger.warning("{} Delete Segment URL: {}".format(datetime.datetime.now().isoformat(), delete_segment_request.url))

    # print(delete_segment_json)
    if delete_segment_request.status_code == 204:
        return "OK"
    else:
        try:
            delete_segment_json = delete_segment_request.json()
            return delete_segment_json["message"]
        except:
            return "Error {}".format(delete_segment_request.status_code)

def edit_segment(access_token, segment_id, data_provider_id, region, category_id, ref_id, fee, ttl, name, status):
    edit_segment_request = requests.put(SEGMENTS_URL + "/" + str(segment_id),
                            headers={
                                "Content-Type":"application/json",
                                "Authorization":"Bearer " + access_token
                            },
                            json={
                                "DataProviderId":data_provider_id,
                                "Status":status,
                                "CategoryId":category_id,
                                "RefId":str(ref_id),
                                "Fee":fee,
                                "Ttl":ttl,
                                "Name":name,
                                "Frequency":1
                            })
    print("Edit Segment URL: {}".format(edit_segment_request.url))
    variables.logger.warning("{} Edit Segment URL: {}".format(datetime.datetime.now().isoformat(), edit_segment_request.url))

    edit_segment_json = edit_segment_request.json()
    # print(add_segment_json)
    if edit_segment_request.status_code == 200:
        segment_id = edit_segment_json["id"]
        return 200, segment_id
    else:
        return None, edit_segment_json["params"]

def store_category_in_dict_by_name(categories_json):
    child_category_dict = {}
    for category in categories_json:
        category_id = category["id"]
        category_name = category["name"]
        category_dataProviderId = category["dataProviderId"]
        category_parentId = None
        if "parentId" in category:
            category_parentId = category["parentId"]
        category_regions = category["regions"]
        category_updatedAt = category["updatedAt"]
        category_createdAt = category["createdAt"]

        child_category_dict[category_id] = {"name":category_name,
                                        "dataProviderId":category_dataProviderId,
                                        "parentId":category_parentId,
                                        "regions":category_regions,
                                        "updatedAt":category_updatedAt,
                                        "createdAt":category_createdAt}

    # Segments could hav esame name but from different data provider id (67 or 11399), hence have to keep them in separate dict
    categories_dict_by_name = {}
    categories_dict_by_name_67 = {}
    categories_dict_by_name_11399 = {}

    for category in categories_json:
        category_id = category["id"]
        category_name = category["name"]
        category_dataProviderId = category["dataProviderId"]
        category_parent_id = None
        if "parentId" in category:
            category_parent_id = category["parentId"]
        category_full_name = get_full_category_name(category_parent_id, category_name, child_category_dict)

        if category_dataProviderId == "67":
            categories_dict_by_name_67[category_full_name] = category_id
        elif category_dataProviderId == "11399":
            categories_dict_by_name_11399[category_full_name] = category_id
    
    categories_dict_by_name["67"] = categories_dict_by_name_67
    categories_dict_by_name["11399"] = categories_dict_by_name_11399

    return categories_dict_by_name

def read_file_to_add_segments(file_path):
    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    access_token = authenticate()
    if "message" in access_token:
        return access_token

    categories_json = get_categories(access_token)
    categories_dict_by_name = store_category_in_dict_by_name(categories_json)

    ref_id_list = read_df["Ref ID"]
    segment_name_list = read_df["Segment Name"]
    region_list = read_df["Region"]
    fee_list = read_df["Fee"]
    ttl_list = read_df["TTL"]
    status_list = read_df["Status"]
    segment_id_list = []
    write_category_result_list = []
    write_add_segment_result_list = []

    row_counter = 0
    for segment_name in segment_name_list:
        region = region_list[row_counter]
        data_provider_id = "67"
        if region.lower() == "apac":
            data_provider_id = "11399"

        status = status_list[row_counter]
        index_of_separator = segment_name.rfind(" - ")

        category_success = True

        # Unable to find separator
        if index_of_separator == -1:
            segment_id_list.append(None)
            write_category_result_list.append("No separator ' - ' is found in the Segment Name")
            write_add_segment_result_list.append(None)
            category_success = False
        else:
            child_segment_name = segment_name[index_of_separator + 3:]
            category_name = segment_name[0:index_of_separator]

            selected_categories_dict_by_name = categories_dict_by_name[data_provider_id]
            if category_name in selected_categories_dict_by_name:
                category_id = selected_categories_dict_by_name[category_name]
            else:
                parent_category_id = None
                category_name_list = category_name.split(" - ")

                # if category does not exist, split the category name and check each and add if not available
                is_parentmost = True
                category_name_to_check = ""
                temp_parent_category_id = None
                for category_part_name in category_name_list:
                    if is_parentmost:
                        category_name_to_check = category_part_name
                    else:
                        category_name_to_check = category_name_to_check + " - " + category_part_name

                    # print("Category Name to Check: {}".format(category_name_to_check))

                    if category_name_to_check in selected_categories_dict_by_name:
                        temp_parent_category_id = selected_categories_dict_by_name[category_name_to_check]
                    else:
                        temp_parent_category_id = add_category(access_token, category_part_name, data_provider_id, region, temp_parent_category_id)

                        if temp_parent_category_id == None:
                            segment_id_list.append(None)
                            write_category_result_list.append("Error creating category: {}".format(category_part_name))
                            write_add_segment_result_list.append(None)
                            category_success = False
                        # category creation success
                        else:
                            selected_categories_dict_by_name[category_name_to_check] = temp_parent_category_id

                    is_parentmost = False

                # if childmost category creation is successful
                if category_success:
                    category_id = temp_parent_category_id

            # category has been created/found, to create segment
            if category_success:
                write_category_result_list.append("OK")

                ref_id = ref_id_list[row_counter]
                fee = fee_list[row_counter]
                ttl = ttl_list[row_counter]

                fee_and_ttl_is_numeric = True
                try:
                    fee = float(fee)
                    ttl = int(ttl)
                except:
                    fee_and_ttl_is_numeric = False
                    segment_id_list.append(None)
                    write_add_segment_result_list.append("TTL and Fee have to be numeric")

                if status.lower() == "active":
                    status = "active"
                else:
                    status = "inactive"
                status_list[row_counter] = status

                if fee_and_ttl_is_numeric:
                    status_code, output = add_segment(access_token, data_provider_id, region, category_id, ref_id, fee, ttl, child_segment_name, status)
                    if status_code == 201:
                        segment_id_list.append(output)
                        write_add_segment_result_list.append("OK")
                    else:
                        segment_id_list.append(None)
                        write_add_segment_result_list.append(output)

        row_counter += 1

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    # print("Ref ID Length: {}".format(len(ref_id_list)))
    # print("Segment Name Length: {}".format(len(segment_name_list)))
    # print("Region Length: {}".format(len(region_list)))
    # print("Fee Length: {}".format(len(fee_list)))
    # print("TTL Length: {}".format(len(ttl_list)))
    # print("Segment ID Length: {}".format(len(segment_id_list)))
    # print("Category Result Length: {}".format(len(write_category_result_list)))
    # print("Add Segment Result Length: {}".format(len(write_add_segment_result_list)))

    write_df = pd.DataFrame({
                        "Segment ID":segment_id_list,
                        "Ref ID":ref_id_list,
                        "Segment Name":segment_name_list,
                        "Region":region_list,
                        "Fee":fee_list,
                        "TTL":ttl_list,
                        "Category Result":write_category_result_list,
                        "Add Segment Result":write_add_segment_result_list
                })
    return write_excel.write(write_df, file_name + "_output_add_segments", SHEET_NAME)

def read_file_to_delete_segments(file_path):
    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    access_token = authenticate()
    if "message" in access_token:
        return access_token
    
    segment_id_list = read_df["Segment ID"]
    ref_id_list = read_df["Ref ID"]
    segment_name_list = read_df["Segment Name"]
    region_list = read_df["Region"]
    fee_list = read_df["Fee"]
    ttl_list = read_df["TTL"]
    status_list = read_df["Status"]
    write_delete_segment_result_list = []

    for segment_id in segment_id_list:
        delete_output = delete_segment(access_token, segment_id)
        write_delete_segment_result_list.append(delete_output)

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    # print("Ref ID Length: {}".format(len(ref_id_list)))
    # print("Segment Name Length: {}".format(len(segment_name_list)))
    # print("Region Length: {}".format(len(region_list)))
    # print("Fee Length: {}".format(len(fee_list)))
    # print("TTL Length: {}".format(len(ttl_list)))
    # print("Segment ID Length: {}".format(len(segment_id_list)))
    # print("Category Result Length: {}".format(len(write_category_result_list)))
    # print("Add Segment Result Length: {}".format(len(write_add_segment_result_list)))

    write_df = pd.DataFrame({
                        "Segment ID":segment_id_list,
                        "Ref ID":ref_id_list,
                        "Segment Name":segment_name_list,
                        "Region":region_list,
                        "Fee":fee_list,
                        "TTL":ttl_list,
                        "Status":status_list,
                        "Delete Segment Result":write_delete_segment_result_list
                })
    return write_excel.write(write_df, file_name + "_output_delete_segments", SHEET_NAME)

def read_file_to_edit_segments(file_path):
    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    access_token = authenticate()
    if "message" in access_token:
        return access_token

    categories_json = get_categories(access_token)
    categories_dict_by_name = store_category_in_dict_by_name(categories_json)

    segment_id_list = read_df["Segment ID"]
    ref_id_list = read_df["Ref ID"]
    segment_name_list = read_df["Segment Name"]
    region_list = read_df["Region"]
    fee_list = read_df["Fee"]
    ttl_list = read_df["TTL"]
    status_list = read_df["Status"]
    write_segment_id_list = []
    write_status_list = []
    write_category_result_list = []
    write_edit_segment_result_list = []

    row_counter = 0
    for segment_name in segment_name_list:
        segment_id = segment_id_list[row_counter]
        region = region_list[row_counter]
        data_provider_id = "67"
        if region.lower() == "apac":
            data_provider_id = "11399"

        status = status_list[row_counter]
        index_of_separator = segment_name.rfind(" - ")

        category_success = True

        # Unable to find separator
        if index_of_separator == -1:
            segment_id_list.append(None)
            write_category_result_list.append("No separator ' - ' is found in the Segment Name")
            write_add_segment_result_list.append(None)
            category_success = False
        else:
            child_segment_name = segment_name[index_of_separator + 3:]
            category_name = segment_name[0:index_of_separator]
            
            selected_categories_dict_by_name = categories_dict_by_name[data_provider_id]
            if category_name in selected_categories_dict_by_name:
                category_id = selected_categories_dict_by_name[category_name]
            else:
                parent_category_id = None
                category_name_list = category_name.split(" - ")

                # if category does not exist, split the category name and check each and add if not available
                is_parentmost = True
                category_name_to_check = ""
                temp_parent_category_id = None
                for category_part_name in category_name_list:
                    if is_parentmost:
                        category_name_to_check = category_part_name
                    else:
                        category_name_to_check = category_name_to_check + " - " + category_part_name

                    # print("Category Name to Check: {}".format(category_name_to_check))

                    if category_name_to_check in selected_categories_dict_by_name:
                        temp_parent_category_id = selected_categories_dict_by_name[category_name_to_check]
                    else:
                        temp_parent_category_id = add_category(access_token, category_part_name, data_provider_id, region, temp_parent_category_id)

                        if temp_parent_category_id == None:
                            segment_id_list.append(None)
                            write_category_result_list.append("Error creating category: {}".format(category_part_name))
                            write_add_segment_result_list.append(None)
                            category_success = False
                        # category creation success
                        else:
                            selected_categories_dict_by_name[category_name_to_check] = temp_parent_category_id

                    is_parentmost = False

                # if childmost category creation is successful
                if category_success:
                    category_id = temp_parent_category_id

            # category has been created/found, to create segment
            if category_success:
                write_category_result_list.append("OK")

                ref_id = ref_id_list[row_counter]
                fee = fee_list[row_counter]
                ttl = ttl_list[row_counter]

                fee_and_ttl_is_numeric = True
                try:
                    fee = float(fee)
                    ttl = int(ttl)
                except:
                    fee_and_ttl_is_numeric = False
                    segment_id_list.append(None)
                    write_edit_segment_result_list.append("TTL and Fee have to be numeric")

                if status.lower() == "active":
                    status = "active"
                else:
                    status = "inactive"
                write_status_list.append(status)

                if fee_and_ttl_is_numeric:
                    status_code, output = edit_segment(access_token, segment_id, data_provider_id, region, category_id, ref_id, fee, ttl, child_segment_name, status)
                    if status_code == 200:
                        write_segment_id_list.append(output)
                        write_edit_segment_result_list.append("OK")
                    else:
                        write_segment_id_list.append(None)
                        write_edit_segment_result_list.append(output)

        row_counter += 1

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    # print("Ref ID Length: {}".format(len(ref_id_list)))
    # print("Segment Name Length: {}".format(len(segment_name_list)))
    # print("Region Length: {}".format(len(region_list)))
    # print("Fee Length: {}".format(len(fee_list)))
    # print("TTL Length: {}".format(len(ttl_list)))
    # print("Segment ID Length: {}".format(len(segment_id_list)))
    # print("Category Result Length: {}".format(len(write_category_result_list)))
    # print("Add Segment Result Length: {}".format(len(write_add_segment_result_list)))

    write_df = pd.DataFrame({
                        "Segment ID":write_segment_id_list,
                        "Ref ID":ref_id_list,
                        "Segment Name":segment_name_list,
                        "Region":region_list,
                        "Fee":fee_list,
                        "TTL":ttl_list,
                        "Status":write_status_list,
                        "Category Result":write_category_result_list,
                        "Edit Segment Result":write_edit_segment_result_list
                })
    return write_excel.write(write_df, file_name + "_output_edit_segments", SHEET_NAME)

def get_data_usage_report(access_token, start_date, end_date, data_provider_id):
    data_usage_report_request = requests.get(DATA_USAGE_REPORT_URL,
                                            headers={
                                                'Content-Type':'application/json',
                                                'Authorization':'Bearer ' + access_token
                                            },
                                            params={
                                                "from":start_date,
                                                "to":end_date,
                                                "dataProviderId":data_provider_id,
                                                "groupBy":"date,country,agency,advertiser,campaign,lineItem,order,dataProvider,segment,source",
                                                "offset":0
                                            })
    print("Get Data Usage Report URL: {}".format(data_usage_report_request.url))
    variables.logger.warning("{} Get Data Usage Report URL: {}".format(datetime.datetime.now().isoformat(), data_usage_report_request.url))

    data_usage_json = data_usage_report_request.json()
    
    return data_usage_json

def get_audience_report(access_token, start_date, end_date, data_provider_id):
    audience_report_request = requests.get(AUDIENCE_REPORT_URL,
                                            headers={
                                                'Content-Type':'application/json',
                                                'Authorization':'Bearer ' + access_token
                                            },
                                            params={
                                                "from":start_date,
                                                "to":end_date,
                                                "dataProviderId":data_provider_id,
                                                "offset":0
                                            })
    print("Get Audience Report URL: {}".format(audience_report_request.url))
    variables.logger.warning("{} Get Audience Report URL: {}".format(datetime.datetime.now().isoformat(), audience_report_request.url))

    audience_json = audience_report_request.json()

    return audience_json

def get_data_provider_name(access_token, data_provider_id):
    get_data_provider_request = requests.get(DATA_PROVIDER_URL + str(data_provider_id),
                                            headers={
                                                'Content-Type':'application/json',
                                                'Authorization':'Bearer ' + access_token
                                            })
    print("Get Data Provider URL: {}".format(get_data_provider_request.url))
    variables.logger.warning("{} Get Data Provider URL: {}".format(datetime.datetime.now().isoformat(), get_data_provider_request.url))
    get_data_provider_json = get_data_provider_request.json()

    return get_data_provider_json["name"]

def read_file_to_get_report(file_path, sheet, report_type):
    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=sheet, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    start_date_list = read_df["Report Start Date"]
    end_date_list = read_df["Report End Date"]

    access_token = authenticate()
    if "message" in access_token:
        return access_token

    row_counter = 0
    file_names = []
    for start_date in start_date_list:
        end_date = end_date_list[row_counter]

        try:
            start_date = start_date.strftime('%Y-%m-%d') + "Z"
        except:
            return {"message":"ERROR: Report Start Date '{}' should be in date format.".format(start_date)}

        try:
            end_date_date_format = datetime.datetime.strptime(str(end_date), "%Y-%m-%d %H:%M:%S") #string to date
            end_date_date_format = end_date_date_format - datetime.timedelta(days=1)

            end_date = end_date_date_format.strftime('%Y-%m-%d') + "Z"
        except:
            return {"message":"ERROR: Report End Date '{}' should be in date format.".format(end_date)}

        write_df = None
        if report_type == "data_usage":
            write_data_provider_name_list = []
            write_advertiser_list = []
            write_advertiserId_list = []
            write_advertiserCurrency_list = []
            write_partnerPlatformCurrency_list = []
            write_country_list = []
            write_countryId_list = []
            write_segmentsGroup_list = []
            write_segmentsGroupId_list = []
            write_segmentIds_list = []
            write_segmentRefIds_list = []
            write_impressions_list = []
            write_revenue_list = []
            write_revenueInAdvertiserCurrency_list = []
            write_revenueInPartnerPlatformCurrency_list = []
            write_revenueInEuro_list = []
            write_dataProviderRevenue_list = []
            write_dataProviderRevenueInAdvertiserCurrency_list = []
            write_dataProviderRevenueInPartnerPlatformCurrency_list = []
            write_dataProviderRevenueInEuro_list = []
            write_adformRevenue_list = []
            write_adformRevenueInAdvertiserCurrency_list = []
            write_adformRevenueInEuro_list = []
            write_segment_list = []
            write_category_list = []

            data_usage_report_response_67_json = get_data_usage_report(access_token, start_date, end_date, '67')
            data_usage_report_response_11399_json = get_data_usage_report(access_token, start_date, end_date, '11399')

            data_provider_name = get_data_provider_name(access_token, 67)
            for data_usage_report_row in data_usage_report_response_67_json:
                # print("Data Usage Report Row: {}".format(data_usage_report_row))
                write_data_provider_name_list.append(data_provider_name)
                write_advertiser_list.append(data_usage_report_row["advertiser"])
                write_advertiserId_list.append(data_usage_report_row["advertiserId"])
                write_advertiserCurrency_list.append(data_usage_report_row["advertiserCurrency"])
                write_partnerPlatformCurrency_list.append(data_usage_report_row["partnerPlatformCurrency"])
                write_country_list.append(data_usage_report_row["country"])
                write_countryId_list.append(data_usage_report_row["countryId"])
                write_segmentsGroup_list.append(data_usage_report_row["segmentsGroup"])
                write_segmentIds_list.append(data_usage_report_row["segmentIds"])
                write_segmentRefIds_list.append(data_usage_report_row["segmentRefIds"])
                write_impressions_list.append(data_usage_report_row["impressions"])
                write_revenue_list.append(data_usage_report_row["revenue"])
                write_revenueInAdvertiserCurrency_list.append(data_usage_report_row["revenueInAdvertiserCurrency"])
                write_revenueInPartnerPlatformCurrency_list.append(data_usage_report_row["revenueInPartnerPlatformCurrency"])
                write_revenueInEuro_list.append(data_usage_report_row["revenueInEuro"])
                write_dataProviderRevenue_list.append(data_usage_report_row["dataProviderRevenue"])
                write_dataProviderRevenueInAdvertiserCurrency_list.append(data_usage_report_row["dataProviderRevenueInAdvertiserCurrency"])
                write_dataProviderRevenueInPartnerPlatformCurrency_list.append(data_usage_report_row["dataProviderRevenueInPartnerPlatformCurrency"])
                write_dataProviderRevenueInEuro_list.append(data_usage_report_row["dataProviderRevenueInEuro"])
                write_adformRevenue_list.append(data_usage_report_row["adformRevenue"])
                write_adformRevenueInAdvertiserCurrency_list.append(data_usage_report_row["adformRevenueInAdvertiserCurrency"])
                write_adformRevenueInEuro_list.append(data_usage_report_row["adformRevenueInEuro"])

            data_provider_name = get_data_provider_name(access_token, 11399)
            for data_usage_report_row in data_usage_report_response_11399_json:
                write_data_provider_name_list.append(data_provider_name)
                write_advertiser_list.append(data_usage_report_row["advertiser"])
                write_advertiserId_list.append(data_usage_report_row["advertiserId"])
                write_advertiserCurrency_list.append(data_usage_report_row["advertiserCurrency"])
                write_partnerPlatformCurrency_list.append(data_usage_report_row["partnerPlatformCurrency"])
                write_country_list.append(data_usage_report_row["country"])
                write_countryId_list.append(data_usage_report_row["countryId"])
                write_segmentsGroup_list.append(data_usage_report_row["segmentsGroup"])
                write_segmentIds_list.append(data_usage_report_row["segmentIds"])
                write_segmentRefIds_list.append(data_usage_report_row["segmentRefIds"])
                write_impressions_list.append(data_usage_report_row["impressions"])
                write_revenue_list.append(data_usage_report_row["revenue"])
                write_revenueInAdvertiserCurrency_list.append(data_usage_report_row["revenueInAdvertiserCurrency"])
                write_revenueInPartnerPlatformCurrency_list.append(data_usage_report_row["revenueInPartnerPlatformCurrency"])
                write_revenueInEuro_list.append(data_usage_report_row["revenueInEuro"])
                write_dataProviderRevenue_list.append(data_usage_report_row["dataProviderRevenue"])
                write_dataProviderRevenueInAdvertiserCurrency_list.append(data_usage_report_row["dataProviderRevenueInAdvertiserCurrency"])
                write_dataProviderRevenueInPartnerPlatformCurrency_list.append(data_usage_report_row["dataProviderRevenueInPartnerPlatformCurrency"])
                write_dataProviderRevenueInEuro_list.append(data_usage_report_row["dataProviderRevenueInEuro"])
                write_adformRevenue_list.append(data_usage_report_row["adformRevenue"])
                write_adformRevenueInAdvertiserCurrency_list.append(data_usage_report_row["adformRevenueInAdvertiserCurrency"])
                write_adformRevenueInEuro_list.append(data_usage_report_row["adformRevenueInEuro"])

            write_df = pd.DataFrame({
                        "dataProviderName":write_data_provider_name_list,
                        "advertiser":write_advertiser_list,
                        "advertiserId":write_advertiserId_list,
                        "advertiserCurency":write_advertiserCurrency_list,
                        "partnerPlatformCurrency":write_partnerPlatformCurrency_list,
                        "country":write_country_list,
                        "countryId":write_countryId_list,
                        "segmentsGroup":write_segmentsGroup_list,
                        "segmentsIds":write_segmentIds_list,
                        "segmentRefIds":write_segmentRefIds_list,
                        "impressions":write_impressions_list,
                        "revenue":write_revenue_list,
                        "revenueInAdvertiserCurrency":write_revenueInAdvertiserCurrency_list,
                        "revenueInPartnerPlatformCurrency":write_revenueInPartnerPlatformCurrency_list,
                        "revenueInEuro":write_revenueInEuro_list,
                        "dataProviderRevenue":write_dataProviderRevenue_list,
                        "dataProviderRevenueInAdvertiserCurrency":write_dataProviderRevenueInAdvertiserCurrency_list,
                        "dataProviderRevenueInPartnerPlatformCurrency":write_dataProviderRevenueInPartnerPlatformCurrency_list,
                        "dataProviderRevenueInEuro":write_dataProviderRevenueInEuro_list,
                        "adformRevenue":write_adformRevenue_list,
                        "adformRevenueInAdvertiserCurrency":write_adformRevenueInAdvertiserCurrency_list,
                        "adformRevenueInEuro":write_adformRevenueInEuro_list,
                    })

            data_usage_file_name = write_excel.write_without_return(write_df, "Adform_data_usage_report_" + str(start_date)[:10] + "_to_" + str(end_date)[:10])
            file_names.append(data_usage_file_name)

        elif report_type == "audience":
            write_data_provider_name = []
            write_date_list = []
            write_segment_id_list = []
            write_category_list = []
            write_segment_list = []
            write_total_list = []
            write_uniques_list = []

            audience_report_response_67_json = get_audience_report(access_token, start_date, end_date, '67')
            audience_report_response_11399_json = get_audience_report(access_token, start_date, end_date, '11399')

            data_provider_name = get_data_provider_name(access_token, 67)
            for audience_report_row in audience_report_response_67_json:
                write_data_provider_name.append(data_provider_name)
                write_date_list.append(audience_report_row["date"])
                write_segment_id_list.append(audience_report_row["segmentId"])
                write_category_list.append(audience_report_row["category"])
                write_segment_list.append(audience_report_row["segment"])
                write_total_list.append(audience_report_row["total"])
                write_uniques_list.append(audience_report_row["uniques"])

            data_provider_name = get_data_provider_name(access_token, 11399)
            for audience_report_row in audience_report_response_11399_json:
                write_data_provider_name.append(data_provider_name)
                write_date_list.append(audience_report_row["date"])
                write_segment_id_list.append(audience_report_row["segmentId"])
                write_category_list.append(audience_report_row["category"])
                write_segment_list.append(audience_report_row["segment"])
                write_total_list.append(audience_report_row["total"])
                write_uniques_list.append(audience_report_row["uniques"])

            write_df = pd.DataFrame({
                        "data_provider":write_data_provider_name,
                        "date":write_date_list,
                        "segment_id":write_segment_id_list,
                        "category":write_category_list,
                        "segment":write_segment_list,
                        "total":write_total_list,
                        "uniques":write_uniques_list
                    })

            audience_file_name = write_excel.write_without_return(write_df, "Adform_audience_report_" + str(start_date)[:10] + "_to_" + str(end_date)[:10])
            file_names.append(audience_file_name)

        row_counter += 1

    return file_names