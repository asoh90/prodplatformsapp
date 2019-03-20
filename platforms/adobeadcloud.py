import requests
import variables
import write_excel
import pandas as pd
import time
import datetime
import os

# URLs
URL = "https://api.tubemogul.com/"
AUTH_URL = URL + "oauth/token"
API_URL = URL + "v3/"
SEGMENT_URL = API_URL + "trafficking/segments"

SHEET_NAME = "Adobe AdCloud"

# Credentials
AUTHORIZATION = "Basic NDkyOTIwOnRBelJqQVhrTTlrbWhlZG1sRk5rWmZEdlZDbWNENEcvMVZQSlIvQkV3YXc9" # base64 encode of 492920:tAzRjAXkM9kmhedmlFNkZfDvVCmcD4G/1VPJR/BEwaw=
GRANT_TYPE = "client_credentials"
CACHE_CONTROL = "no-cache"

# Segments
LIMIT = 50
PARTNER = "eyeota"

def callAPI(function, file_path):
    if function == "Add Custom Segments":
        # output = read_all_to_add_segments(file_path)
        output = read_all_to_add_segments(file_path)
    elif function == "Edit Custom Segments":
        output = read_all_to_edit_segments(file_path)
    
    return output

def authenticate():
    data = {'grant_type':GRANT_TYPE}
    auth_request = requests.post(AUTH_URL,
                headers={
                    'Content-Type':"application/x-www-form-urlencoded",
                    'Authorization':AUTHORIZATION,
                    'Cache-Control':CACHE_CONTROL
                },
                data=data)
    print("Authentication URL: {}".format(auth_request.url))
    variables.logger.warning("{} Authentication URL: {}".format(datetime.datetime.now().isoformat(), auth_request.url))
        
    auth_json = auth_request.json()
    return auth_json["token"]

def check_token_timeout(start_time, token):
    now = time.time()
    elapsed_seconds = now - start_time
    elapsed_minutes = elapsed_seconds / 60
    if elapsed_minutes > 58:
        new_token = authenticate()
        return now, new_token
    else:
        return start_time, token

def read_all_to_add_segments(file_path):
    start_time = time.time()
    token = authenticate()
    
    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    account_token_list = read_df["Account"]
    segment_id_list = read_df["Eyeota Segment ID"]
    segment_name_list = read_df["Segment Name"]
    segment_description_list = read_df["Segment Description"]
    cpm_list = read_df["CPM"]
    lifetime_list = read_df["Lifetime"]
    write_remarks_list = []

    row_counter = 0
    for segment_name in segment_name_list:
        account_token = account_token_list[row_counter]
        segment_id = segment_id_list[row_counter]
        segment_description = segment_description_list[row_counter]

        try:
            cpm = float(cpm_list[row_counter])
            lifetime = int(lifetime_list[row_counter])

            add_segment_request = requests.post(SEGMENT_URL,
                                    headers={
                                        'Content-Type':'application/json',
                                        'Authorization':"Bearer " + token
                                    },
                                    json={
                                        "account_token":account_token,
                                        "partner": PARTNER,
                                        "segment_name": segment_name,
                                        "segment_description": segment_description,
                                        "partner_segment_id": str(segment_id),
                                        "billing_through_tubemogul":"F",
                                        "cpm": cpm,
                                        "retention_window_in_days": lifetime
                                    }
                                )
            print("Add Segment URL: {}".format(add_segment_request.url))
            variables.logger.warning("{} Add Segment URL: {}".format(datetime.datetime.now().isoformat(), add_segment_request.url))

            add_segment_json = add_segment_request.json()
            if add_segment_request.status_code == 200:
                write_remarks_list.append(add_segment_json["segment_id"])
            else:
                write_remarks_list.append(add_segment_json)
        except:
            write_remarks_list.append("CPM is not decimal or Lifetime is not integer")

        row_counter += 1

        # Check if token is expired
        start_time, token = check_token_timeout(start_time, token)

    write_df = pd.DataFrame({
        "Account":account_token_list,
        "Eyeota Segment ID":segment_id_list,
        "Segment Name":segment_name_list,
        "Segment Description":segment_description_list,
        "CPM":cpm_list,
        "Lifetime":lifetime_list,
        "Remarks":write_remarks_list
    })

    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]
    os.remove(file_path)

    return write_excel.write(write_df, file_name + "output_add_segments", SHEET_NAME)

def read_all_to_edit_segments(file_path):
    start_time = time.time()
    token = authenticate()
    
    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    account_token_list = read_df["Account"]
    segment_id_list = read_df["Eyeota Segment ID"]
    segment_name_list = read_df["Segment Name"]
    segment_description_list = read_df["Segment Description"]
    status_list = read_df["Status"]
    cpm_list = read_df["CPM"]
    lifetime_list = read_df["Lifetime"]
    adcloud_segment_id_list = read_df["AdCloud Segment ID"]
    write_remarks_list = []

    row_counter = 0
    for segment_id in segment_id_list:
        account_token = account_token_list[row_counter]
        segment_name = segment_name_list[row_counter]
        segment_description = segment_description_list[row_counter]
        

        try:
            status = status_list[row_counter]
            if status:
                status = "active"
            else:
                status = "inactive"
            cpm = float(cpm_list[row_counter])
            lifetime = int(lifetime_list[row_counter])

            edit_segment_request = requests.put(SEGMENT_URL + "/" + str(segment_id),
                                    headers={
                                        'Content-Type':'application/json',
                                        'Authorization':"Bearer " + token
                                    },
                                    json={
                                        "account_token":account_token,
                                        "partner": PARTNER,
                                        "segment_name": segment_name,
                                        "segment_description": segment_description,
                                        "partner_segment_id": str(segment_id),
                                        "billing_through_tubemogul":"F",
                                        "cpm": cpm,
                                        "retention_window_in_days": lifetime,
                                        "status": status
                                    }
                                )
            print("Edit Segment URL: {}".format(edit_segment_request.url))
            variables.logger.warning("{} Edit Segment URL: {}".format(datetime.datetime.now().isoformat(), edit_segment_request.url))

            if edit_segment_request.status_code == 200:
                write_remarks_list.append("Edited successfully")
            else:
                write_remarks_list.append(edit_segment_request.json())
        except:
            write_remarks_list.append("CPM is not decimal or Lifetime is not integer")

        row_counter += 1

        # Check if token is expired
        start_time, token = check_token_timeout(start_time, token)

    write_df = pd.DataFrame({
        "Account":account_token_list,
        "Eyeota Segment ID":segment_id_list,
        "Segment Name":segment_name_list,
        "Segment Description":segment_description_list,
        "Status": status_list,
        "CPM":cpm_list,
        "Lifetime":lifetime_list,
        "Remarks":write_remarks_list
    })

    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]
    os.remove(file_path)

    return write_excel.write(write_df, file_name + "output_edit_segments", SHEET_NAME)