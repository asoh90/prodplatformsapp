import requests
import write_excel
import variables
import pandas as pd
import os
import numpy
import time
from flask import session
import datetime

MEMBER_ID = 1706
THREAD_LIMIT = 50

# API URL
url_home  = None
url_auth = None
url_segment = None
url_buyer_member_data_sharing = None
url_segment_billing_category = None
url_report = None

# Folder to retrieve uploaded file
UPLOAD_FOLDER = variables.UPLOAD_FOLDER

# Login credentials
login = None
password = None

auth_token = None
RETRIEVE_SEGMENTS_NUM_ELEMENTS = 100
AUTHENTICATION_LIMIT_SECS = 235 * 60

SHEET_NAME = "AppNexus"

appnexus_pool = None

def callAPI(platform, function, file_path, loop):
    get_urls_output = get_urls(platform)
    if "message" in get_urls_output:
        return get_urls_output

    output = {"message":"ERROR: option is not available"}
    auth_token = authenticate()
    if "message" in auth_token:
        return auth_token
    
    start_time = time.time()
    if function == "Add Segments":
        output = read_file_to_add_segments(file_path)
    elif function == "Edit Segments":
        output = read_file_to_edit_segments(file_path)
    elif function == "Query All Segments":
        output = query_all_segments()
    elif function == "Retrieve Segments":
        output = read_file_to_retrieve_segments(file_path)
    elif function == "Add Existing Segments to Specific Buyer Member":
        output = read_file_to_add_existing_segments_to_buyer_member(file_path)
    elif function == "Add Segment Billings":
        output = read_file_to_add_segment_billings(file_path)
    elif function == "Retrieve Buyer Member Segments":
        output = read_file_to_retrieve_buyer_member_segments(file_path)
    elif function == "Segment Loads Report":
        file_names = read_file_to_get_report(file_path, "segment_loads", SHEET_NAME, None)
        output = write_excel.return_report(file_names, SHEET_NAME, file_path)
    elif function == "Data Usage Report":
        segment_dict = retrieve_all_segments()
        file_names = read_file_to_get_report(file_path, "data_usage", SHEET_NAME, segment_dict)
        output = write_excel.return_report(file_names, SHEET_NAME, file_path)
    elapsed_time = time.time() - start_time
    elapsed_mins = int(elapsed_time/60)
    elapsed_secs = int(elapsed_time%60)

    print("Elapsed time: {} mins {} secs".format(elapsed_mins, elapsed_secs))

    return output

def get_urls(platform):
    global url_home
    if platform == "AppNexus":
        url_home = 'https://api.appnexus.com/'
    elif platform == "AppNexus Staging":
        url_home = 'https://api-test.appnexus.com/'
    else:
        return {"message":"ERROR: platform {} is not available in appnexus.py".format(platform)}

    global url_auth; url_auth = url_home + "auth"
    global url_segment; url_segment = url_home + "segment"
    global url_buyer_member_data_sharing; url_buyer_member_data_sharing = url_home + "member-data-sharing"
    global url_segment_billing_category; url_segment_billing_category = url_home + "segment-billing-category"
    global url_report; url_report = url_home + "report"
    global url_download_report; url_download_report = url_home + "report-download"

    global appnexus_pool; appnexus_pool = variables.thread_pool_dict[SHEET_NAME]

    return {}


def authenticate():
    try:
        # Login credentials
        global login; login = variables.login_credentials['AppNexus']['Login']
        global password; password = variables.login_credentials['AppNexus']['PW']
    except:
        return {"message":"ERROR: Incorrect login credentials! Please download 'asoh-flask-deploy.sh' file from <a href='https://eyeota.atlassian.net/wiki/pages/viewpageattachments.action?pageId=127336529&metadataLink=true'>Confluence</a> again!>"}

    auth_credentials = {'username':login,'password':password}
    auth_request = requests.post(url_auth,
                              headers={
                                  'Content-Type':'application/json'
                              },
                              json={
                                  'auth':auth_credentials
                              })
    print("Authenticate URL: {}".format(auth_request.url))
    variables.logger.warning("{} Authentication URL: {}".format(datetime.datetime.now().isoformat(), auth_request.url))
    auth_json = auth_request.json()
    response = auth_json['response']
    # print(response)
    global auth_token; auth_token = response['token']
    return auth_token

# Start Query Segments functions
def query_all_segments():
    # print("Query all segments now")
    write_segment_id_list = []
    write_code_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_price_list = []
    write_duration_list = []
    write_member_id_list = []
    write_state_list = []
    write_last_modified_list = []
    write_is_public_list = []
    write_data_segment_type_id_list = []
    write_data_category_id_list = []

    segment_billing_dict = get_all_segment_billing()
    segment_dict = retrieve_all_segments()
    
    for segment_id in segment_dict:
        segment = segment_dict[segment_id]

        write_segment_id_list.append(segment_id)
        write_code_list.append(segment["code"])
        write_segment_name_list.append(segment["short_name"])
        write_segment_description_list.append(segment["description"])
        write_price_list.append(segment["price"])
        write_duration_list.append(segment["expire_minutes"])
        write_member_id_list.append(segment["member_id"])
        write_state_list.append(segment["state"])
        write_last_modified_list.append(segment["last_modified"])

        if segment_id in segment_billing_dict:
            segment_billing = segment_billing_dict[segment_id]
            write_is_public_list.append(segment_billing["is_public"])
            write_data_segment_type_id_list.append(segment_billing["data_segment_type_id"])
            write_data_category_id_list.append(segment_billing["data_category_id"])
        else:
            write_is_public_list.append(None)
            write_data_segment_type_id_list.append(None)
            write_data_category_id_list.append(None)

    write_df = pd.DataFrame({
                    "Segment ID":write_segment_id_list,
                    'code':write_code_list,
                    'Segment Name':write_segment_name_list,
                    'Segment Description':write_segment_description_list,
                    'Price':write_price_list,
                    'Duration':write_duration_list,
                    'State':write_state_list,
                    'Is Public':write_is_public_list,
                    'Data Segment Type ID':write_data_segment_type_id_list,
                    'Data Category ID':write_data_category_id_list,
                    'Member ID':write_member_id_list,
                    'Last Modified':write_last_modified_list
                })
    return write_excel.write_and_email(write_df, "DONOTUPLOAD_AppNexus_query_all", SHEET_NAME)
    # return write_excel.write(write_df, "DONOTUPLOAD_AppNexus_query_all", SHEET_NAME)

def retrieve_all_segments():
    segment_dict = {}

    total_segments = [1]
    # start_element = 0

    global appnexus_pool
    start_element = 0

    while start_element < total_segments[0]:
        thread_counter = 0

        while thread_counter < THREAD_LIMIT and start_element < total_segments[0]:
            appnexus_pool.add_task(retrieve_segments, start_element, RETRIEVE_SEGMENTS_NUM_ELEMENTS, segment_dict, total_segments)
            start_element += RETRIEVE_SEGMENTS_NUM_ELEMENTS
            thread_counter += 1

    appnexus_pool.wait_completion()

    return segment_dict


def retrieve_segments(start_element, num_elements, segment_dict, total_segments):
    request_to_send = requests.get(url_segment,
                                headers={
                                    'Content-Type':'application/json',
                                    'Authorization':auth_token
                                },
                                params={
                                    "start_element":start_element,
                                    "num_elements":num_elements,
                                    "member_id":MEMBER_ID
                                })
    print("Retrieve Request: {}".format(request_to_send.url))
    variables.logger.warning("{} Retrieve Request: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
    retrieve_response = request_to_send.json()
    # print(retrieve_response)

    if (total_segments[0] == 1):
        total_segments[0] = retrieve_response["response"]["count"]
    print("Retrieving {} of {} segments".format(start_element, total_segments))
    variables.logger.warning("{} Retrieving {} of {} segments".format(datetime.datetime.now().isoformat(), start_element, total_segments))

    # print(retrieve_response)
    try:
        for segment in retrieve_response['response']['segments']:
            segment_id = segment["id"]
            code = segment["code"]
            segment_name = segment["short_name"]
            segment_description = segment["description"]
            price = segment["price"]
            duration = segment["expire_minutes"]
            member_id = segment["member_id"]
            state = segment["state"]
            last_modified = segment["last_modified"]

            segment_dict[segment_id] = {
                                            "code":code,
                                            "short_name":segment_name,
                                            "description":segment_description,
                                            "price":price,
                                            "expire_minutes":duration,
                                            "member_id":member_id,
                                            "state":state,
                                            "last_modified":last_modified                
            }
    except:
        print("ERROR: {}".format(retrieve_response['response']['error']))
        variables.logger.warning("{} ERROR: {}".format(datetime.datetime.now().isoformat(), retrieve_response['response']['error']))
        print("Error storing segment to segment_dict. Wait 20 seconds to run again.")
        variables.logger.warning("{} Error storing segment to segment_dict. Wait 20 seconds to run again.")
        time.sleep(20)
        retrieve_segments(start_element, num_elements, segment_dict, total_segments)
    finally:
        print("Sleep for 20 seconds to avoid call limit")
        variables.logger.warning("{} Sleep for 20 seconds to avoid call limit".format(datetime.datetime.now().isoformat()))
        time.sleep(20)
# End Query Segments functions

# Start Add Segments functions
# Many input fields due to multithreading. current_segments will then be called again so that all the data are aligned
def add_segment(code, segment_name, segment_description, price, duration, state, is_public, data_segment_type_id, data_category_id, buyer_member_id, current_segments, output_messages):
    segment_to_add = {
                        "code":str(code),
                        "expire_minutes":int(duration),
                        "short_name":segment_name,
                        "description":str(segment_description),
                        "price":float(price),
                        "state":state
                    }
    try:
        request_to_send = requests.post(url_segment,
                                    headers={
                                        'Content-Type':'application/json',
                                        'Authorization':auth_token
                                    },
                                    params={
                                        'member_id':MEMBER_ID
                                    },
                                    json={
                                        'segment':segment_to_add
                                    })
        print("Add Request: {}".format(request_to_send.url))
        variables.logger.warning("{} Add Request: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        add_response = request_to_send.json()
        # print(add_response)
        response = add_response["response"]
        segment_id = response["id"]
        current_segments[code] = {
                                    "segment_id":segment_id,
                                    "segment_name":segment_name,
                                    "segment_description":segment_description,
                                    "price":price,
                                    "duration":duration,
                                    "state":state,
                                    "is_public":is_public,
                                    "data_segment_type_id":data_segment_type_id,
                                    "data_category_id":data_category_id,
                                    "buyer_member_id":buyer_member_id
                                }
        # print("NEW Segment ID: {}".format(segment_id))
        output_messages[code] = "OK"
    except:
        current_segments[code] = None
        try:
            error_message = response["error"]
            output_messages[code] = error_message
        except:
            output_messages[code] = response

def read_file_to_add_segments(file_path):
    add_segments_start_time = time.time()
    add_segments_authenticate_count = 1
    global appnexus_pool

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}
    
    code_list = read_df["code"]
    segment_name_list = read_df["Segment Name"]
    segment_description_list = read_df["Segment Description"]
    price_list = read_df["Price"]
    duration_list = read_df["Duration"]
    state_list = read_df["State"]
    is_public_list = read_df["Is Public"]
    data_segment_type_id_list = read_df["Data Segment Type ID"]
    data_category_id_list = read_df["Data Category ID"]
    buyer_member_id_list = read_df["Buyer Member ID"]

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    write_segment_id_list = []
    write_code_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_price_list = []
    write_duration_list = []
    write_member_id_list = []
    write_state_list = []
    write_is_public_list = []
    write_data_segment_type_id_list = []
    write_data_category_id_list = []
    write_buyer_member_id_list = []
    write_response = []
    write_billing_response = []

    private_segment_list = {}

    add_segment_row_num = 0
    add_billing_row_num = 0

    batch_num = 1

    while add_segment_row_num < len(code_list):
        add_segment_thread_counter = 0
        current_segments = {}
        output_messages = {}
        add_billing_outputs = {}

        add_segment_threads = []
        add_billing_threads = []

        while add_segment_thread_counter < THREAD_LIMIT and add_segment_row_num < len(code_list):
            current_code = code_list[add_segment_row_num]
            current_segment_name = segment_name_list[add_segment_row_num]
            current_segment_description = segment_description_list[add_segment_row_num]
            current_price = price_list[add_segment_row_num]
            current_duration = duration_list[add_segment_row_num]
            current_state = state_list[add_segment_row_num]
            current_is_public = is_public_list[add_segment_row_num]
            current_data_segment_type_id = data_segment_type_id_list[add_segment_row_num]
            current_data_category_id = data_category_id_list[add_segment_row_num]
            current_buyer_member_id = buyer_member_id_list[add_segment_row_num]

            # print("Adding {} segment".format(str(add_segment_row_num+1)))

            # print("Adding code: {}".format(current_code))
            # add_segment_process = Thread(target=add_segment, args=[current_code, current_segment_name, current_segment_description, current_price, current_duration, current_state, current_is_public, current_data_segment_type_id, current_data_category_id, current_buyer_member_id, current_segments, output_messages])
            # add_segment_process.start()
            # add_segment_threads.append(add_segment_process)
            appnexus_pool.add_task(add_segment, current_code, current_segment_name, current_segment_description, current_price, current_duration, current_state, current_is_public, current_data_segment_type_id, current_data_category_id, current_buyer_member_id, current_segments, output_messages)

            add_segment_thread_counter += 1
            add_segment_row_num += 1

        # for add_segment_thread in add_segment_threads:
        #     add_segment_thread.join()
        appnexus_pool.wait_completion()
            
        add_billing_thread_counter = 0

        while add_billing_thread_counter < THREAD_LIMIT and add_billing_row_num < len(code_list):
            add_billing_code = code_list[add_billing_row_num]
            add_billing_segment_name = segment_name_list[add_billing_row_num]
            add_billing_segment_description = segment_description_list[add_billing_row_num]
            add_billing_price = price_list[add_billing_row_num]
            add_billing_duration = duration_list[add_billing_row_num]
            add_billing_state = state_list[add_billing_row_num]
            add_billing_is_public = is_public_list[add_billing_row_num]
            add_billing_data_segment_type_id = data_segment_type_id_list[add_billing_row_num]
            add_billing_data_category_id = data_category_id_list[add_billing_row_num]
            add_billing_buyer_member_id = buyer_member_id_list[add_billing_row_num]

            add_billing_segment_id = None
            add_billing_segment = current_segments[add_billing_code]
            if not add_billing_segment == None:
                add_billing_segment_id = add_billing_segment["segment_id"]
            # print("current_segment_id: {}".format(current_segment_id))

            # Private segments will append to a list to be added to specific buyer member
            if not add_billing_is_public and not add_billing_segment_id == None:
                # Buyer member does not already have a list
                
                if not add_billing_buyer_member_id in private_segment_list:
                    private_segment_list[add_billing_buyer_member_id] = {}

                buyer_member_private_segment = private_segment_list[add_billing_buyer_member_id]
                buyer_member_private_segment[add_billing_segment_id] = {
                                                            "segment_id":add_billing_segment_id,
                                                            "code":add_billing_code,
                                                            "segment_name":add_billing_segment_name,
                                                            "segment_description":add_billing_segment_description,
                                                            "price":add_billing_price,
                                                            "duration":add_billing_duration,
                                                            "state":add_billing_state,
                                                            "is_public":add_billing_is_public,
                                                            "segment_id_type":add_billing_data_segment_type_id,
                                                            "data_category_id":add_billing_data_category_id,
                                                            "response":None
                                                        }
            # Public segments can add response to the list
            else:
                if add_billing_segment_id == None:
                    add_billing_outputs[add_billing_code] = None
                else:
                    # add_billing_process = Thread(target=add_segment_billing, args=[add_billing_segment_id, add_billing_code, add_billing_state, add_billing_data_category_id, add_billing_is_public, add_billing_data_segment_type_id, add_billing_outputs])
                    # add_billing_process.start()
                    # add_billing_threads.append(add_billing_process)
                    appnexus_pool.add_task(add_segment_billing, add_billing_segment_id, add_billing_code, add_billing_state, add_billing_data_category_id, add_billing_is_public, add_billing_data_segment_type_id, add_billing_outputs)

            add_billing_thread_counter += 1
            add_billing_row_num += 1

        # for add_billing_thread in add_billing_threads:
        #     add_billing_thread.join()
        appnexus_pool.wait_completion()

        for after_add_billing_code in add_billing_outputs:
            after_add_billing_segment = current_segments[after_add_billing_code]

            after_add_billing_segment_id = None
            after_add_billing_segment_name = None
            after_add_billing_segment_description = None
            after_add_billing_price = None
            after_add_billing_duration = None
            after_add_billing_state = None
            after_add_billing_is_public = None
            after_add_billing_data_segment_type_id = None
            after_add_billing_data_category_id = None
            after_add_billing_buyer_member_id = None

            if not after_add_billing_segment == None:
                after_add_billing_segment_id = after_add_billing_segment["segment_id"]
                after_add_billing_segment_name = after_add_billing_segment["segment_name"]
                after_add_billing_segment_description = after_add_billing_segment["segment_description"]
                after_add_billing_price = after_add_billing_segment["price"]
                after_add_billing_duration = after_add_billing_segment["duration"]
                after_add_billing_state = after_add_billing_segment["state"]
                after_add_billing_is_public = after_add_billing_segment["is_public"]
                after_add_billing_data_segment_type_id = after_add_billing_segment["data_segment_type_id"]
                after_add_billing_data_category_id = after_add_billing_segment["data_category_id"]
                after_add_billing_buyer_member_id = after_add_billing_segment["buyer_member_id"]

            after_add_billing_response = output_messages[after_add_billing_code]
            after_add_billing_billing_response = add_billing_outputs[after_add_billing_code]

            write_segment_id_list.append(after_add_billing_segment_id)
            write_code_list.append(after_add_billing_code)
            write_segment_name_list.append(after_add_billing_segment_name)
            write_segment_description_list.append(after_add_billing_segment_description)
            write_price_list.append(after_add_billing_price)
            write_duration_list.append(after_add_billing_duration)
            write_member_id_list.append(MEMBER_ID)
            write_state_list.append(after_add_billing_state)
            write_is_public_list.append(after_add_billing_is_public)
            write_data_segment_type_id_list.append(after_add_billing_data_segment_type_id)
            write_data_category_id_list.append(after_add_billing_data_category_id)
            write_buyer_member_id_list.append(after_add_billing_buyer_member_id)
            write_response.append(after_add_billing_response)
            write_billing_response.append(after_add_billing_billing_response)

        if add_segment_row_num < len(code_list):
            print("Sleep 60 seconds to avoid limit")
            variables.logger.warning("{} Sleep 60 seconds to avoid limit".format(datetime.datetime.now().isoformat()))
            time.sleep(60)

            add_segments_current_time = time.time()
            add_segments_elapsed_secs = add_segments_current_time - add_segments_start_time
            add_segments_authentication_timeover = add_segments_elapsed_secs - AUTHENTICATION_LIMIT_SECS * add_segments_authenticate_count

            if add_segments_authentication_timeover > 0:
                print("Authenticating...")
                variables.logger.warning("{} Authenticating".format(datetime.datetime.now().isoformat()))
                authenticate()
                add_segments_authenticate_count += 1

    private_batch_num = 1
    # Add private segments to specific buyer_member_id
    private_segments_to_add = list(private_segment_list.keys())
    if len(private_segments_to_add) > 0:
        private_segment_row_num = 0
        private_segment_thread_counter = 0
        private_segment_billing_outputs = {}
        private_segment_billing_threads = []
        private_segments = {}

        # for buyer_member_id in private_segments_to_add:
        while private_segment_row_num < len(private_segments_to_add):

            while private_segment_thread_counter < THREAD_LIMIT and private_segment_row_num < len(private_segments_to_add):
                buyer_member_id = private_segments_to_add[private_segment_row_num]
                buyer_member_private_segment_list = private_segment_list[buyer_member_id]
                private_segment_response = refresh_segments(buyer_member_id, buyer_member_private_segment_list)
                # print(private_segment_response)

                for segment_id in private_segment_response:
                    private_segment_details = private_segment_response[segment_id]
                    private_segment_code = private_segment_details["code"]

                    private_segments[private_segment_code] = {
                                                                "segment_id":private_segment_details["segment_id"],
                                                                "segment_name":private_segment_details["segment_name"],
                                                                "segment_description":private_segment_details["segment_description"],
                                                                "price":private_segment_details["price"],
                                                                "duration":private_segment_details["duration"],
                                                                "state":private_segment_details["state"],
                                                                "is_public":private_segment_details["is_public"],
                                                                "data_segment_type_id":private_segment_details["segment_id_type"],
                                                                "data_category_id":private_segment_details["data_category_id"],
                                                                "buyer_member_id":buyer_member_id,
                                                                "response":private_segment_details["response"]
                                                            }

                    # private_segment_billing_process = Thread(target=add_segment_billing, args=[private_segment_details["segment_id"], private_segment_code, private_segment_details["state"], private_segment_details["data_category_id"], private_segment_details["is_public"], private_segment_details["segment_id_type"], private_segment_billing_outputs])
                    # private_segment_billing_process.start()
                    # private_segment_billing_threads.append(private_segment_billing_process)
                    appnexus_pool.add_task(add_segment_billing, private_segment_details["segment_id"], private_segment_code, private_segment_details["state"], private_segment_details["data_category_id"], private_segment_details["is_public"], private_segment_details["segment_id_type"], private_segment_billing_outputs)

                private_segment_thread_counter += 1
                private_segment_row_num += 1

            # for private_segment_billing_thread in private_segment_billing_threads:
            #     private_segment_billing_thread.join()
            appnexus_pool.wait_completion()

            # if len(private_segment_billing_outputs) > 0:
            #     print("Sleep for 60 seconds for billing to be updated")
            #     time.sleep(60)

            for private_segment_code in private_segments:
                private_segment = private_segments[private_segment_code]

                write_segment_id_list.append(private_segment["segment_id"])
                write_code_list.append(private_segment_code)
                write_segment_name_list.append(private_segment["segment_name"])
                write_segment_description_list.append(private_segment["segment_description"])
                write_price_list.append(private_segment["price"])
                write_duration_list.append(private_segment["duration"])
                write_member_id_list.append(MEMBER_ID)
                write_state_list.append(private_segment["state"])
                write_is_public_list.append(private_segment["is_public"])
                write_data_segment_type_id_list.append(private_segment["data_segment_type_id"])
                write_data_category_id_list.append(private_segment["data_category_id"])
                write_buyer_member_id_list.append(private_segment["buyer_member_id"])
                write_response.append(private_segment["response"])

                private_segment_billing_response = private_segment_billing_outputs[private_segment_code]
                write_billing_response.append(private_segment_billing_response)

            if private_segment_row_num < len(private_segments_to_add):
                if private_batch_num % 2 == 0:
                    print("Sleep 60 seconds to avoid limit")
                    variables.logger.warning("{} Sleep 60 seconds to avoid limit".format(datetime.datetime.now().isoformat()))
                    time.sleep(60)

                private_segments_current_time = time.time()
                private_segments_elapsed_secs = private_segments_current_time - add_segments_start_time
                private_segments_authentication_timeover = private_segments_elapsed_secs - AUTHENTICATION_LIMIT_SECS * add_segments_authenticate_count

                if private_segments_authentication_timeover > 0:
                    print("Authenticating...")
                    variables.logger.warning("{} Authenticating...".format(datetime.datetime.now().isoformat()))
                    authenticate()
                    add_segments_authenticate_count += 1
            
            private_batch_num += 1

    # Print result of creating segments
    # print("Segment ID len: {}".format(len(write_segment_id_list)))
    # print("code len: {}".format(len(write_code_list)))
    # print("Segment Name len: {}".format(len(write_segment_name_list)))
    # print("Segment Description len: {}".format(len(write_segment_description_list)))
    # print("Price len: {}".format(len(write_price_list)))
    # print("Duration len: {}".format(len(write_duration_list)))
    # print("Member ID len: {}".format(len(write_member_id_list)))
    # print("State len: {}".format(len(write_state_list)))
    # print("Is Public len: {}".format(len(write_is_public_list)))
    # print("Data Segment Type ID len: {}".format(len(write_data_segment_type_id_list)))
    # print("Data Category ID len: {}".format(len(write_data_category_id_list)))
    # print("Buyer Member ID len: {}".format(len(write_buyer_member_id_list)))
    # print("Add Segment Response len: {}".format(len(write_response)))
    # print("Add Billing Response len: {}".format(len(write_billing_response)))


    write_df = pd.DataFrame({
                    "Segment ID":write_segment_id_list,
                    'code':write_code_list,
                    'Segment Name':write_segment_name_list,
                    'Segment Description':write_segment_description_list,
                    'Price':write_price_list,
                    'Duration':write_duration_list,
                    'State':write_state_list,
                    'Is Public':write_is_public_list,
                    'Data Segment Type ID':write_data_segment_type_id_list,
                    'Data Category ID':write_data_category_id_list,
                    'Buyer Member ID':write_buyer_member_id_list,
                    'Member ID':write_member_id_list,
                    'Add Segment Response':write_response,
                    'Add Billing Response':write_billing_response
                })
    return write_excel.write(write_df, file_name + "_output_add_segments", SHEET_NAME)

# # Many input fields due to multithreading. current_segments will then be called again so that all the data are aligned
def edit_segment(segment_id, code, segment_name, segment_description, price, duration, state, is_public, data_segment_type_id, data_category_id, buyer_member_id, current_segments, output_messages):
    if len(segment_description) > 500:
        segment_description = segment_description[0:500]
        # print("Segment Description: {}".format(segment_description))

    segment_to_edit = {
                        "member_id":MEMBER_ID,
                        "code":str(code),
                        "expire_minutes":int(duration),
                        "short_name":segment_name,
                        "description":str(segment_description),
                        "price":float(price),
                        "state":state
                    }
    # print(segment_to_edit)
    try:
        request_to_send = requests.put(url_segment,
                                    headers={
                                        'Content-Type':'application/json',
                                        'Authorization':auth_token
                                    },
                                    params={
                                        'member_id':MEMBER_ID,
                                        'id':segment_id
                                    },
                                    json={
                                        'segment':segment_to_edit
                                    })
        print("Edit Request: {}".format(request_to_send.url))
        variables.logger.warning("{} Edit Request: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        edit_response = request_to_send.json()
        # print(edit_response)
        response = edit_response["response"]
        current_segments[code] = {
                                    "segment_id":segment_id,
                                    "segment_name":segment_name,
                                    "segment_description":segment_description,
                                    "price":price,
                                    "duration":duration,
                                    "state":state,
                                    "is_public":is_public,
                                    "data_segment_type_id":data_segment_type_id,
                                    "data_category_id":data_category_id,
                                    "buyer_member_id":buyer_member_id
                                }
        output_messages[code] = "OK"
    except:
        current_segments[code] = None

        try:
            error_message = response["error"]
            output_messages[code] = error_message
        except:
            output_messages[code] = response

def read_file_to_edit_segments(file_path):
    edit_segments_start_time = time.time()
    edit_segments_authenticate_count = 1
    global appnexus_pool

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}
    
    segment_id_list = read_df["Segment ID"]
    code_list = read_df["code"]
    segment_name_list = read_df["Segment Name"]
    segment_description_list = read_df["Segment Description"]
    price_list = read_df["Price"]
    duration_list = read_df["Duration"]
    state_list = read_df["State"]
    is_public_list = read_df["Is Public"]
    data_segment_type_id_list = read_df["Data Segment Type ID"]
    data_category_id_list = read_df["Data Category ID"]
    buyer_member_id_list = read_df["Buyer Member ID"]

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    write_segment_id_list = []
    write_code_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_price_list = []
    write_duration_list = []
    write_member_id_list = []
    write_state_list = []
    write_is_public_list = []
    write_data_segment_type_id_list = []
    write_data_category_id_list = []
    write_buyer_member_id_list = []
    write_response = []
    write_billing_response = []

    edit_segment_row_num = 0
    edit_billing_row_num = 0
    get_billing_row_num = 0

    batch_num = 1

    while edit_segment_row_num < len(code_list):
        edit_segment_thread_counter = 0
        current_segments = {}
        current_segment_billings = {}
        output_messages = {}
        get_billing_outputs = {}
        edit_billing_outputs = {}

        edit_segment_threads = []
        edit_billing_threads = []
        get_billing_threads = []

        while edit_segment_thread_counter < THREAD_LIMIT and edit_segment_row_num < len(code_list):
            current_segment_id = segment_id_list[edit_segment_row_num]
            current_code = code_list[edit_segment_row_num]
            current_segment_name = segment_name_list[edit_segment_row_num]
            current_segment_description = segment_description_list[edit_segment_row_num]
            current_price = price_list[edit_segment_row_num]
            current_duration = duration_list[edit_segment_row_num]
            current_state = state_list[edit_segment_row_num]
            current_is_public = is_public_list[edit_segment_row_num]
            current_data_segment_type_id = data_segment_type_id_list[edit_segment_row_num]
            current_data_category_id = data_category_id_list[edit_segment_row_num]
            current_buyer_member_id = buyer_member_id_list[edit_segment_row_num]

            # edit_segment_process = Thread(target=edit_segment, args=[current_segment_id, current_code, current_segment_name, current_segment_description, current_price, current_duration, current_state, current_is_public, current_data_segment_type_id, current_data_category_id, current_buyer_member_id, current_segments, output_messages])
            # edit_segment_process.start()
            # edit_segment_threads.append(edit_segment_process)
            appnexus_pool.add_task(edit_segment, current_segment_id, current_code, current_segment_name, current_segment_description, current_price, current_duration, current_state, current_is_public, current_data_segment_type_id, current_data_category_id, current_buyer_member_id, current_segments, output_messages)

            edit_segment_thread_counter += 1
            edit_segment_row_num += 1
        
        # for edit_segment_thread in edit_segment_threads:
        #     edit_segment_thread.join()
        appnexus_pool.wait_completion()

        get_billing_thread_counter = 0

        while get_billing_thread_counter < THREAD_LIMIT and get_billing_row_num < len(code_list):
            get_billing_code = code_list[get_billing_row_num]

            get_billing_segment_id = None
            if not current_segments[get_billing_code] == None:
                get_billing_segment_id = current_segments[get_billing_code]["segment_id"]
            
            if not get_billing_segment_id == None:
                get_billing_process = Thread(target=get_segment_billing,args=[get_billing_segment_id, get_billing_code, current_segment_billings, get_billing_outputs])
                get_billing_process.start()
                get_billing_threads.append(get_billing_process)
            # else:
            #     print("get_billing_code: {}".format(get_billing_code))
            #     print("get_billing_segment_id: {}".format(get_billing_segment_id))

            get_billing_thread_counter += 1
            get_billing_row_num += 1

        for get_billing_thread in get_billing_threads:
            get_billing_thread.join()

        edit_billing_thread_counter = 0

        while edit_billing_thread_counter < THREAD_LIMIT and edit_billing_row_num < len(code_list):
            edit_billing_code = code_list[edit_billing_row_num]
            edit_billing_state = state_list[edit_billing_row_num]
            edit_billing_data_category_id = data_category_id_list[edit_billing_row_num]
            edit_billing_is_public = is_public_list[edit_billing_row_num]
            edit_billing_data_segment_type_id = data_segment_type_id_list[edit_billing_row_num]
            
            edit_billing_segment_id = None
            if edit_billing_code in current_segments:
                edit_billing_segment_id = current_segments[edit_billing_code]["segment_id"]

            edit_billing_segment_billing_id = None
            segment_billing = current_segment_billings[edit_billing_code]
            if not segment_billing == None:
                edit_billing_segment_billing_id = segment_billing["id"]

            if edit_billing_segment_id == None:
                edit_billing_outputs[edit_billing_code] = "Unable to retrieve segment id"
            elif segment_billing == None:
                edit_billing_outputs[edit_billing_code] = get_billing_outputs[edit_billing_code]
            else:
                # segment_billing_id, segment_id, code, state, data_category_id, is_public, data_segment_type_id, edit_billing_outputs
                # edit_billing_process = Thread(target=edit_segment_billing, args=[edit_billing_segment_billing_id, edit_billing_segment_id, edit_billing_code, edit_billing_state, edit_billing_data_category_id, edit_billing_is_public, edit_billing_data_segment_type_id, edit_billing_outputs])
                # edit_billing_process.start()
                # edit_billing_threads.append(edit_billing_process)
                appnexus_pool.add_task(edit_segment_billing, edit_billing_segment_billing_id, edit_billing_segment_id, edit_billing_code, edit_billing_state, edit_billing_data_category_id, edit_billing_is_public, edit_billing_data_segment_type_id, edit_billing_outputs)

            edit_billing_thread_counter += 1
            edit_billing_row_num += 1
        
        # for edit_billing_thread in edit_billing_threads:
        #     edit_billing_thread.join()
        appnexus_pool.wait_completion()

        for after_edit_billing_code in edit_billing_outputs:
            after_edit_billing_segment = current_segments[after_edit_billing_code]

            after_edit_billing_segment_id = None
            after_edit_billing_segment_name = None
            after_edit_billing_segment_description = None
            after_edit_billing_price = None
            after_edit_billing_duration = None
            after_edit_billing_state = None
            after_edit_billing_is_public = None
            after_edit_billing_data_segment_type_id = None
            after_edit_billing_data_category_id = None
            after_edit_billing_buyer_member_id = None
            after_edit_billing_response = output_messages[after_edit_billing_code]
            after_edit_billing_billing_response = edit_billing_outputs[after_edit_billing_code]

            if not after_edit_billing_segment == None:
                after_edit_billing_segment_id = after_edit_billing_segment["segment_id"]
                after_edit_billing_segment_name = after_edit_billing_segment["segment_name"]
                after_edit_billing_segment_description = after_edit_billing_segment["segment_description"]
                after_edit_billing_price = after_edit_billing_segment["price"]
                after_edit_billing_duration = after_edit_billing_segment["duration"]
                after_edit_billing_state = after_edit_billing_segment["state"]
                after_edit_billing_is_public = after_edit_billing_segment["is_public"]
                after_edit_billing_data_segment_type_id = after_edit_billing_segment["data_segment_type_id"]
                after_edit_billing_data_category_id = after_edit_billing_segment["data_category_id"]
                after_edit_billing_buyer_member_id = after_edit_billing_segment["buyer_member_id"]

            write_segment_id_list.append(after_edit_billing_segment_id)
            write_code_list.append(after_edit_billing_code)
            write_segment_name_list.append(after_edit_billing_segment_name)
            write_segment_description_list.append(after_edit_billing_segment_description)
            write_price_list.append(after_edit_billing_price)
            write_duration_list.append(after_edit_billing_duration)
            write_member_id_list.append(MEMBER_ID)
            write_state_list.append(after_edit_billing_state)
            write_is_public_list.append(after_edit_billing_is_public)
            write_data_segment_type_id_list.append(after_edit_billing_data_segment_type_id)
            write_data_category_id_list.append(after_edit_billing_data_category_id)
            write_buyer_member_id_list.append(after_edit_billing_buyer_member_id)
            write_response.append(after_edit_billing_response)
            write_billing_response.append(after_edit_billing_billing_response)

        if edit_segment_row_num < len(code_list):
            print("Sleep 60 seconds to avoid limit")
            variables.logger.warning("{} Sleep 60 seconds to avoid limit".format(datetime.datetime.now().isoformat()))
            time.sleep(60)

            edit_segments_current_time = time.time()
            edit_segments_elapsed_secs = edit_segments_current_time - edit_segments_start_time
            edit_segments_authentication_timeover = edit_segments_elapsed_secs - AUTHENTICATION_LIMIT_SECS * edit_segments_authenticate_count

            if edit_segments_authentication_timeover > 0:
                print("Authenticating...")
                variables.logger.warning("{} Authenticating...".format(datetime.datetime.now().isoformat()))
                authenticate()
                edit_segments_authenticate_count += 1

    write_df = pd.DataFrame({
                "Segment ID":write_segment_id_list,
                'code':code_list,
                'Segment Name':segment_name_list,
                'Segment Description':segment_description_list,
                'Price':price_list,
                'Duration':duration_list,
                'State':state_list,
                'Is Public':is_public_list,
                'Data Segment Type ID':data_segment_type_id_list,
                'Data Category ID':data_category_id_list,
                'Buyer Member ID':buyer_member_id_list,
                'Member ID':write_member_id_list,
                'Edit Segment Response':write_response,
                'Edit Billing Response':write_billing_response
            })
        
    return write_excel.write(write_df, "DONOTUPLOAD_" + file_name + "_edit_segments", SHEET_NAME)

# overwrites segments for specific buyer member id
def refresh_segments(buyer_member_id, buyer_member_private_segment_list):
    retrieve_results = retrieve_segments_for_member(buyer_member_id)
    record_id = retrieve_results["record_id"]
    current_segment_list = retrieve_results["segment_list"]
    if current_segment_list == None:
        return
    current_segment_id_list = []
    updated_segment_list = []
    
    for current_segment in current_segment_list:
        current_segment_id_list.append(current_segment["id"])
        updated_segment_list.append({"id":current_segment["id"]})

    for private_segment in buyer_member_private_segment_list:
        if private_segment not in current_segment_id_list:
            updated_segment_list.append({"id":buyer_member_private_segment_list[private_segment]["segment_id"]})
            current_segment_id_list.append(buyer_member_private_segment_list[private_segment]["segment_id"])

    response = refresh_segment_ids(record_id, updated_segment_list)

    for private_segment in buyer_member_private_segment_list:
        buyer_member_private_segment_list[private_segment]["response"] = response

    return buyer_member_private_segment_list

def retrieve_segments_for_member(buyer_member_id):
    buyer_member_id = int(buyer_member_id)
    try:
        request_to_send = requests.get(url_buyer_member_data_sharing,
                                            headers={
                                                'Content-Type':'application/json',
                                                'Authorization':auth_token
                                            },
                                            params={
                                                'data_member_id':MEMBER_ID,
                                                'buyer_member_id':buyer_member_id
                                            }
                                        )
        print("Retrieve Request URL: {}".format(request_to_send.url))
        variables.logger.warning("{} Retrieve Request URL: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        retrieve_response = request_to_send.json()
        member_data_sharings = retrieve_response["response"]["member_data_sharings"][0]
        segment_list = member_data_sharings["segments"]
        record_id = member_data_sharings["id"]
        print("Record ID: {}".format(record_id))
        variables.logger.warning("{} Record ID: {}".format(datetime.datetime.now().isoformat(), record_id))
        return {"record_id":record_id,"segment_list":segment_list}
    except Exception:
        print(retrieve_response["response"]["error"])
        variables.logger.warning("{} ERROR: {}".format(datetime.datetime.now().isoformat(), retrieve_response["response"]["error"]))

# overwrite segment ids for specific record_id
def refresh_segment_ids(record_id, new_segment_id_list):
    print("Refreshing record id: {}".format(record_id))
    variables.logger.warning("{} Refreshing record id: {}".format(datetime.datetime.now().isoformat(), record_id))

    segment_list_to_send = {"segments":new_segment_id_list}

    try:
        request_to_send = requests.put(url_buyer_member_data_sharing,
                                                headers={
                                                    'Content-Type':'application/json',
                                                    'Authorization':auth_token
                                                },
                                                params={
                                                    'id':record_id
                                                },
                                                json={
                                                    'member_data_sharing':segment_list_to_send
                                                })
        print("Refresh Request URL: {}".format(request_to_send.url))
        variables.logger.warning("{} Refresh Request URL: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        refresh_response = request_to_send.json()
        return refresh_response["response"]["status"]
    except Exception as e:
        if request_to_send.status_code == 504:
            return "timed out"

        return refresh_response["response"]["error"]
# End Add Segments functions

def retrieve_segment(code, current_segments, output_messages):
    try:
        request_to_send = requests.get(url_segment,
                                            headers={
                                                'Content-Type':'application/json',
                                                'Authorization':auth_token
                                            },
                                            params={
                                                'code':code
                                            }
                                        )
        print("Retrieve Segment ID Request URL: " + request_to_send.url)
        variables.logger.warning("{} Retrieve Segment ID Request URL: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        retrieve_response = request_to_send.json()
        segment = retrieve_response["response"]["segment"]
        current_segments[code] = {
                                    "segment_id":segment["id"],
                                    "segment_name":segment["short_name"],
                                    "description":segment["description"],
                                    "price":segment["price"],
                                    "duration":segment["expire_minutes"],
                                    "member_id":segment["member_id"]
                                }
        output_messages[code] = "OK"
    except Exception:
        current_segments[code] = None
        try:
            output_messages[code] = retrieve_response["response"]['error']
        except:
            output_messages[code] = retrieve_response

# def retrieve_segment_by_id(segment_id):
#     segment_id = segment_id
#     try:
#         request_to_send = requests.get(url_segment,
#                                             headers={
#                                                 'Content-Type':'application/json',
#                                                 'Authorization':auth_token
#                                             },
#                                             params={
#                                                 'id':segment_id
#                                             }
#                                         )
#         print("Retrieve Segment ID Request URL: " + request_to_send.url)
#         retrieve_response = request_to_send.json()
#         segment = retrieve_response["response"]["segment"]
#         return segment
#     except Exception:
#         return "Unable to find segment id {}".format(code)

def read_file_to_add_existing_segments_to_buyer_member(file_path):
    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_name)}
    
    segment_id_list = read_df["Segment ID"]
    code_list = read_df["code"]
    segment_name_list = read_df["Segment Name"]
    segment_description_list = read_df["Segment Description"]
    price_list = read_df["Price"]
    duration_list = read_df["Duration"]
    state_list = read_df["State"]
    is_public_list = read_df["Is Public"]
    data_segment_type_id_list = read_df["Data Segment Type ID"]
    data_category_id_list = read_df["Data Category ID"]
    buyer_member_id_list = read_df["Buyer Member ID"]

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    write_segment_id_list = []
    write_code_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_price_list = []
    write_duration_list = []
    write_member_id_list = []
    write_state_list = []
    write_is_public_list = []
    write_data_segment_type_id_list = []
    write_data_category_id_list = []
    write_buyer_member_id_list = []
    write_response = []

    private_segment_dict = {}

    # try:
    for row_num in range(len(segment_id_list)):
        current_segment_id = int(segment_id_list[row_num])
        current_code = code_list[row_num]
        current_segment_name = segment_name_list[row_num]
        current_segment_description = segment_description_list[row_num]
        current_price = price_list[row_num]
        current_duration = duration_list[row_num]
        current_state = state_list[row_num]
        current_is_public = is_public_list[row_num]
        current_data_segment_type_id = data_segment_type_id_list[row_num]
        current_data_category_id = data_category_id_list[row_num]
        current_buyer_member_id = buyer_member_id_list[row_num]

        if not current_buyer_member_id in private_segment_dict:
            private_segment_dict[current_buyer_member_id] = {}

        buyer_member_private_segment = private_segment_dict[current_buyer_member_id]

        buyer_member_private_segment[current_segment_id] = {
                                                    "segment_id":current_segment_id,
                                                    "code":current_code,
                                                    "segment_name":current_segment_name,
                                                    "segment_description":current_segment_description,
                                                    "price":current_price,
                                                    "duration":current_duration,
                                                    "state":current_state,
                                                    "is_public":current_is_public,
                                                    "data_segment_type_id":current_data_segment_type_id,
                                                    "data_category_id":current_data_category_id,
                                                    "response":None
                                                }

    private_segments_to_add = private_segment_dict.keys()
    if len(private_segments_to_add) > 0:
        for buyer_member_id in private_segments_to_add:
            buyer_member_private_segment_list = private_segment_dict[buyer_member_id]
            private_segment_response = refresh_segments(buyer_member_id, buyer_member_private_segment_list)

            for segment_id in private_segment_response:
                private_segment_details = private_segment_response[segment_id]
                write_segment_id_list.append(private_segment_details["segment_id"])
                write_code_list.append(private_segment_details["code"])
                write_segment_name_list.append(private_segment_details["segment_name"])
                write_segment_description_list.append(private_segment_details["segment_description"])
                write_price_list.append(private_segment_details["price"])
                write_duration_list.append(private_segment_details["duration"])
                write_member_id_list.append(MEMBER_ID)
                write_state_list.append(private_segment_details["state"])
                write_is_public_list.append(private_segment_details["is_public"])
                write_data_segment_type_id_list.append(private_segment_details["data_segment_type_id"])
                write_data_category_id_list.append(private_segment_details["data_category_id"])
                write_buyer_member_id_list.append(buyer_member_id)
                write_response.append(private_segment_details["response"])

    # Print result of creating segments
    write_df = pd.DataFrame({
                    "Segment ID":write_segment_id_list,
                    'code':write_code_list,
                    'Segment Name':write_segment_name_list,
                    'Segment Description':write_segment_description_list,
                    'Price':write_price_list,
                    'Duration':write_duration_list,
                    'State':write_state_list,
                    'Is Public':write_is_public_list,
                    'Data Segment Type ID':write_data_segment_type_id_list,
                    'Data Category ID':write_data_category_id_list,
                    'Buyer Member ID':write_buyer_member_id_list,
                    'Response':write_response
                })
    return write_excel.write(write_df, "DONOTUPLOAD_" + file_name + "_add_to_buyer", SHEET_NAME)
    # except:
    #     return {"message":"ERROR in the data file. Check if Segment IDs are all present."}

def read_file_to_retrieve_buyer_member_segments(file_path):
    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]
    
    buyer_member_id_list = read_df["Buyer Member ID"]

    write_segment_id_list = []
    write_segment_name_list = []
    write_buyer_member_id_list = []

    unique_buyer_id_list = []
    for buyer_id in buyer_member_id_list:
        if not buyer_id in unique_buyer_id_list and not numpy.isnan(buyer_id):
            unique_buyer_id_list.append(buyer_id)

    for buyer_member_id in unique_buyer_id_list:
        try:
            buyer_member_segments = retrieve_segments_for_member(buyer_member_id)["segment_list"]
        
            for buyer_member_segment in buyer_member_segments:
                write_buyer_member_id_list.append(buyer_member_id)
                write_segment_id_list.append(buyer_member_segment["id"])
                write_segment_name_list.append(buyer_member_segment["name"])
                
        except:
            write_buyer_member_id_list.append(buyer_member_id)
            write_segment_id_list.append("Buyer Member ID: {} is invalid!".format(buyer_member_id))
            write_segment_name_list.append("")
    
    # Print result of creating segments
    write_df = pd.DataFrame({
                    "Segment ID":write_segment_id_list,
                    'Segment Name':write_segment_name_list,
                    "Buyer Member ID":write_buyer_member_id_list
                })
    return write_excel.write(write_df, "DONOTUPLOAD_" + file_name + "_buyer_segments", SHEET_NAME)

# Start of Segment Billing Functions
def read_file_to_add_segment_billings(file_path):
    add_billing_start_time = time.time()
    add_billing_authenticate_count = 1
    global appnexus_pool

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}
    
    segment_id_list = read_df["Segment ID"]
    code_list = read_df["code"]
    segment_name_list = read_df["Segment Name"]
    segment_description_list = read_df["Segment Description"]
    price_list = read_df["Price"]
    duration_list = read_df["Duration"]
    state_list = read_df["State"]
    is_public_list = read_df["Is Public"]
    data_segment_type_id_list = read_df["Data Segment Type ID"]
    data_category_id_list = read_df["Data Category ID"]
    buyer_member_id_list = read_df["Buyer Member ID"]
    
    write_segment_id_list = []
    write_code_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_price_list = []
    write_duration_list = []
    write_member_id_list = []
    write_state_list = []
    write_is_public_list = []
    write_data_segment_type_id_list = []
    write_data_category_id_list = []
    write_buyer_member_id_list = []
    write_billing_response = []

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    add_billing_row_num = 0
    add_billing_outputs = {}

    batch_num = 1

    while add_billing_row_num < len(segment_id_list):
        add_billing_thread_counter = 0
        add_billing_threads = []
        current_segments = {}

        while add_billing_thread_counter < THREAD_LIMIT and add_billing_row_num < len(segment_id_list):
            add_billing_segment_id = segment_id_list[add_billing_row_num]
            add_billing_code = code_list[add_billing_row_num]
            add_billing_segment_name = segment_name_list[add_billing_row_num]
            add_billing_segment_description = segment_description_list[add_billing_row_num]
            add_billing_price = price_list[add_billing_row_num]
            add_billing_duration = duration_list[add_billing_row_num]
            add_billing_state = state_list[add_billing_row_num]
            add_billing_is_public = is_public_list[add_billing_row_num]
            add_billing_data_segment_type_id = data_segment_type_id_list[add_billing_row_num]
            add_billing_data_category_id = data_category_id_list[add_billing_row_num]
            add_billing_buyer_member_id = buyer_member_id_list[add_billing_row_num]

            current_segments[add_billing_code] = {
                                                    "segment_id":add_billing_segment_id,
                                                    "segment_name":add_billing_segment_name,
                                                    "segment_description":add_billing_segment_description,
                                                    "price":add_billing_price,
                                                    "duration":add_billing_duration,
                                                    "state":add_billing_state,
                                                    "is_public":add_billing_is_public,
                                                    "data_segment_type_id":add_billing_data_segment_type_id,
                                                    "data_category_id":add_billing_data_category_id,
                                                    "buyer_member_id":add_billing_buyer_member_id
                                                }

            # add_billing_process = Thread(target=add_segment_billing, args=[add_billing_segment_id, add_billing_code, add_billing_state, add_billing_data_category_id, add_billing_is_public, add_billing_data_segment_type_id, add_billing_outputs])
            # add_billing_process.start()
            # add_billing_threads.append(add_billing_process)
            appnexus_pool.add_task(add_segment_billing, add_billing_segment_id, add_billing_code, add_billing_state, add_billing_data_category_id, add_billing_is_public, add_billing_data_segment_type_id, add_billing_outputs)

            add_billing_row_num += 1
            add_billing_thread_counter += 1

        # for add_billing_thread in add_billing_threads:
        #     add_billing_thread.join()
        appnexus_pool.wait_completion()

        for after_add_billing_code in current_segments:
            after_add_billing_segment = current_segments[after_add_billing_code]

            after_add_billing_segment_id = after_add_billing_segment["segment_id"]
            after_add_billing_segment_name = after_add_billing_segment["segment_name"]
            after_add_billing_segment_description = after_add_billing_segment["segment_description"]
            after_add_billing_price = after_add_billing_segment["price"]
            after_add_billing_duration = after_add_billing_segment["duration"]
            after_add_billing_state = after_add_billing_segment["state"]
            after_add_billing_is_public = after_add_billing_segment["is_public"]
            after_add_billing_data_segment_type_id = after_add_billing_segment["data_segment_type_id"]
            after_add_billing_data_category_id = after_add_billing_segment["data_category_id"]
            after_add_billing_buyer_member_id = after_add_billing_segment["buyer_member_id"]
            after_add_billing_billing_response = add_billing_outputs[after_add_billing_code]

            write_segment_id_list.append(after_add_billing_segment_id)
            write_code_list.append(after_add_billing_code)
            write_segment_name_list.append(after_add_billing_segment_name)
            write_segment_description_list.append(after_add_billing_segment_description)
            write_price_list.append(after_add_billing_price)
            write_duration_list.append(after_add_billing_duration)
            write_member_id_list.append(MEMBER_ID)
            write_state_list.append(after_add_billing_state)
            write_is_public_list.append(after_add_billing_is_public)
            write_data_segment_type_id_list.append(after_add_billing_data_segment_type_id)
            write_data_category_id_list.append(after_add_billing_data_category_id)
            write_buyer_member_id_list.append(after_add_billing_buyer_member_id)
            write_billing_response.append(after_add_billing_billing_response)

        if add_billing_row_num < len(segment_id_list):
            print("Sleep 60 seconds to avoid limit")
            variables.logger.warning("{} Sleep 60 seconds to avoid limit".format(datetime.datetime.now().isoformat()))
            time.sleep(60)

            add_billing_current_time = time.time()
            add_billing_elapsed_secs = add_billing_current_time - add_billing_start_time
            add_billing_authentication_timeover = add_billing_elapsed_secs - AUTHENTICATION_LIMIT_SECS * add_billing_authenticate_count

            if add_billing_authentication_timeover > 0:
                print("Authenticating...")
                variables.logger.warning("{} Authenticating...".format(datetime.datetime.now().isoformat()))
                authenticate()
                add_billing_authenticate_count += 1


    write_df = pd.DataFrame({
                    "Segment ID":write_segment_id_list,
                    'code':write_code_list,
                    'Segment Name':write_segment_name_list,
                    'Segment Description':write_segment_description_list,
                    'Price':write_price_list,
                    'Duration':write_duration_list,
                    'Member ID':write_member_id_list,
                    'State':write_state_list,
                    'Is Public':write_is_public_list,
                    "Data Segment Type ID":write_data_segment_type_id_list,
                    'Data Category ID':write_data_category_id_list,
                    'Buyer Member ID':write_buyer_member_id_list,
                    'Billing Response':write_billing_response
                })
    return write_excel.write(write_df, "DONOTUPLOAD_" + file_name + "_add_billing", SHEET_NAME)

def get_segment_loads_report(start_date, end_date):
    request_json = {
                        "report":{
                                    "report_type":"segment_load",
                                    "columns":["segment_id","segment_name","month","total_loads","monthly_uniques","avg_daily_uniques"],
                                    "groups":["segment_id","month"],
                                    "orders":["month"],
                                    "format":"excel",
                                    "start_date":str(start_date),
                                    "end_date":str(end_date)
                        }
                    }
    request_segment_loads_report = requests.post(url_report,
                                    headers={
                                        "Content-Type":"application/json",
                                        'Authorization':auth_token
                                    },
                                    json=request_json)
    print("Get Segments Loads Report URL: {}".format(request_segment_loads_report.url))
    variables.logger.warning("{} Get Segments Loads Report URL: {}".format(datetime.datetime.now().isoformat(), request_segment_loads_report.url))
    
    response = request_segment_loads_report.json()
    if response["response"]["status"] == "error":
        return "error", response["response"]["error"]
    else:
        return "OK", response["response"]["report_id"]

def get_data_usage_report(start_date, end_date):
    request_json = {
                        "report":{
                                    "report_type":"data_usage_analytics_for_data_providers",
                                    "columns":["geo_country",
                                                "day",
                                                "buyer_member_name",
                                                "campaign_id",
                                                "campaign_name",
                                                "targeted_segment_ids",
                                                "cpm_usd",
                                                "imps",
                                                "data_costs",
                                                "data_clearing_fee_usd",
                                                "data_provider_payout_usd"],
                                    "groups":["day"],
                                    "orders":["day"],
                                    "format":"excel",
                                    "start_date":str(start_date),
                                    "end_date":str(end_date)
                        }
                    }
    request_data_usage_report = requests.post(url_report,
                                    headers={
                                        "Content-Type":"application/json",
                                        'Authorization':auth_token
                                    },
                                    json=request_json)
    print("Get Data Usage Report URL: {}".format(request_data_usage_report.url))
    variables.logger.warning("{} Get Data Usage Report URL: {}".format(datetime.datetime.now().isoformat(), request_data_usage_report.url))
    
    response = request_data_usage_report.json()
    if response["response"]["status"] == "error":
        return "error", response["response"]["error"]
    else:
        return "OK", response["response"]["report_id"]

def download_report(report_id):
    print("Sleep 5 seconds before retrieving report")
    variables.logger.warning("{} Sleep 5 seconds before retrieving report".format(datetime.datetime.now().isoformat()))
    # print("REPORT ID: {}".format(report_id))
    time.sleep(5)
    request_report = requests.get(url_download_report,
                    params={
                        "id":report_id
                    },
                    headers={
                        'Authorization':auth_token
                    })
    request_report_string = request_report.content
    request_report_string_list = request_report_string.split(b"\r\n")
    # print("Length of report: {}".format(len(request_report_string_list)))
    # print(request_report_string_list)
    return request_report_string_list

def read_file_to_get_report(file_path, report_type, sheet, segment_dict):
    auth_token = authenticate()
    if "message" in auth_token:
        return auth_token

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=sheet, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}

    start_date_list = read_df["Report Start Date"]
    end_date_list = read_df["Report End Date"]

    file_names = []

    row_counter = 0
    for start_date in start_date_list:
        end_date = end_date_list[row_counter]
        file_name = get_report(start_date, end_date, report_type, row_counter, segment_dict)
        file_names.append(file_name)

        row_counter += 1

    return file_names

def get_report(start_date, end_date, report_type, row_counter, segment_dict):
    is_report_line_header = True

    if report_type == "segment_loads":
        report_status, report_response = get_segment_loads_report(start_date, end_date)
        report_id = report_response
        report_data = download_report(report_id)

        segment_id_list = []
        segment_name_list = []
        month_list = []
        total_loads_list = []
        monthly_uniques_list = []
        avg_daily_uniques_list = []

        # report is returned in string
        for report_line in report_data:
            if is_report_line_header:
                is_report_line_header = False
                continue

            report_line_data = report_line.split(b"\t")
            # print(report_line_data)
            if len(report_line_data) > 1:
                segment_id_list.append(report_line_data[0])
                segment_name_list.append(report_line_data[1])
                month_list.append(report_line_data[2])
                total_loads_list.append(report_line_data[3])
                monthly_uniques_list.append(report_line_data[4])
                avg_daily_uniques_list.append(report_line_data[5])

        write_df = pd.DataFrame({
            "segment_id":segment_id_list,
            "segment_name":segment_name_list,
            "month":month_list,
            "total_loads":total_loads_list,
            "monthly_uniques":monthly_uniques_list,
            "avg_daily_uniques":avg_daily_uniques_list
        })

        return write_excel.write_without_return(write_df, "AppNexus_segment_loads_report_" + str(start_date)[:10] + "_to_" + str(end_date)[:10])

    elif report_type == "data_usage":
        report_status, report_response = get_data_usage_report(start_date, end_date)
        report_id = report_response
        report_data = download_report(report_id)

        geo_country_list = []
        day_list = []
        buyer_member_name_list = []
        campaign_id_list = []
        campaign_name_list = []
        targeted_segment_id_list = []
        targeted_segment_name_list = []
        cpm_usd_list = []
        imps_list = []
        data_costs_list = []
        data_clearing_fee_usd_list = []
        data_provider_payout_usd_list = []

        for report_line in report_data:
            if is_report_line_header:
                is_report_line_header = False
                continue

            report_line_data = report_line.split(b"\t")

            if len(report_line_data) > 1:
                geo_country_list.append(report_line_data[0])
                day_list.append(report_line_data[1])
                buyer_member_name_list.append(report_line_data[2])
                campaign_id_list.append(report_line_data[3])
                campaign_name_list.append(report_line_data[4])

                targeted_segment_ids = report_line_data[5]
                # targeted_segment_id_list.append(targeted_segment_ids.decode('utf-8'))
                targeted_segment_ids_split = targeted_segment_ids.split(b",")
                formatted_targeted_segment_names = ""
                formatted_targeted_segment_ids = ""

                is_first_segment = True
                for targeted_segment_id in targeted_segment_ids_split:
                    targeted_segment_name = segment_dict[int(targeted_segment_id)]["short_name"]
                    targeted_segment_id = int(targeted_segment_id)

                    if is_first_segment:
                        formatted_targeted_segment_names = targeted_segment_name
                        formatted_targeted_segment_ids = str(targeted_segment_id)
                        is_first_segment = False
                    else:
                        formatted_targeted_segment_names = formatted_targeted_segment_names + ";" + targeted_segment_name
                        formatted_targeted_segment_ids = formatted_targeted_segment_ids + ";" + str(targeted_segment_id)
                targeted_segment_name_list.append(formatted_targeted_segment_names)
                targeted_segment_id_list.append(formatted_targeted_segment_ids)

                cpm_usd_list.append(report_line_data[6])
                imps_list.append(report_line_data[7])
                data_costs_list.append(report_line_data[8])
                data_clearing_fee_usd_list.append(report_line_data[9])
                data_provider_payout_usd_list.append(report_line_data[10])

        write_df = pd.DataFrame({
            "geo_country":geo_country_list,
            "day":day_list,
            "buyer_member_name":buyer_member_name_list,
            "campaign_id":campaign_id_list,
            "campaign_name":campaign_name_list,
            "targeted_segment_ids":targeted_segment_id_list,
            "targeted_segment_names":targeted_segment_name_list,
            "cpm_usd":cpm_usd_list,
            "imps":imps_list,
            "data_costs":data_costs_list,
            "data_clearing_fee_usd":data_clearing_fee_usd_list,
            "data_provider_payout_usd":data_provider_payout_usd_list
        })

        return write_excel.write_without_return(write_df, "AppNexus_data_usage_report_" + str(start_date)[:10] + "_to_" + str(end_date)[:10])

def read_file_to_retrieve_segments(file_path):
    get_segments_start_time = time.time()
    get_segments_authenticate_count = 1
    global appnexus_pool

    read_df = None
    try:
        # Skip row 2 ([1]) tha indicates if field is mandatory or not
        read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    except:
        return {"message":"File Path '{}' is not found".format(file_path)}
    
    code_list = read_df["code"]

    write_segment_id_list = []
    write_code_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_price_list = []
    write_duration_list = []
    write_member_id_list = []
    write_state_list = []
    write_is_public_list = []
    write_data_segment_type_id_list = []
    write_data_category_id_list = []
    write_response = []
    write_billing_response = []

    os.remove(file_path)
    file_name_with_extension = file_path.split("/")[-1]
    file_name = file_name_with_extension.split(".xlsx")[0]

    get_segment_row_num = 0
    batch_num = 1

    while get_segment_row_num < len(code_list):
        get_segment_threads = []
        get_segment_thread_counter = 0
        current_segments = {}
        output_messages = {}

        while get_segment_thread_counter < THREAD_LIMIT and get_segment_row_num < len(code_list):
            current_code = code_list[get_segment_row_num]
            # get_segment_process = Thread(target=retrieve_segment, args=[current_code, current_segments, output_messages])
            # get_segment_process.start()
            # get_segment_threads.append(get_segment_process)
            appnexus_pool.add_task(retrieve_segment, current_code, current_segments, output_messages)

            get_segment_thread_counter += 1
            get_segment_row_num += 1
        
        # for get_segment_thread in get_segment_threads:
        #     get_segment_thread.join()
        appnexus_pool.wait_completion()

        get_billing_threads = []
        get_billing_thread_counter = 0
        get_billing_row_num = 0
        current_segment_billings = {}
        billing_output_messages = {}
        get_billing_code_list = list(current_segments.keys())

        while get_billing_thread_counter < THREAD_LIMIT and get_billing_row_num < len(get_billing_code_list):
            get_billing_segment_code = get_billing_code_list[get_billing_row_num]
            get_billing_segment = current_segments[get_billing_segment_code]

            if not get_billing_segment == None:
                get_billing_segment_id = get_billing_segment["segment_id"]
                # get_billing_process = Thread(target=get_segment_billing, args=[get_billing_segment_id, get_billing_segment_code, current_segment_billings, billing_output_messages])
                # get_billing_process.start()
                # get_billing_threads.append(get_billing_process)
                appnexus_pool.add_task(get_segment_billing, get_billing_segment_id, get_billing_segment_code, current_segment_billings, billing_output_messages)
            else:
                billing_output_messages[get_billing_segment_code] = "Unable to retrieve segment id for code {}".format(get_billing_segment_code)

            get_billing_thread_counter += 1
            get_billing_row_num += 1

        # for get_billing_thread in get_billing_threads:
        #     get_billing_thread.join()
        appnexus_pool.wait_completion()

        for after_billing_code in current_segments:
            after_billing_segment = current_segments[after_billing_code]
            if not after_billing_segment == None:
                write_segment_id_list.append(after_billing_segment["segment_id"])
                write_code_list.append(after_billing_code)
                write_segment_name_list.append(after_billing_segment["segment_name"])
                write_segment_description_list.append(after_billing_segment["description"])
                write_price_list.append(after_billing_segment["price"])
                write_duration_list.append(after_billing_segment["duration"])
                write_member_id_list.append(MEMBER_ID)
            else:
                write_segment_id_list.append(None)
                write_code_list.append(after_billing_code)
                write_segment_name_list.append(None)
                write_segment_description_list.append(None)
                write_price_list.append(None)
                write_duration_list.append(None)
                write_member_id_list.append(None)
            write_response.append(output_messages[after_billing_code])

            after_billing_segment_billing = None
            if after_billing_code in current_segment_billings:
                after_billing_segment_billing = current_segment_billings[after_billing_code]
            
            if not after_billing_segment_billing == None:
                write_state_list.append(after_billing_segment_billing["state"])
                write_is_public_list.append(after_billing_segment_billing["is_public"])
                write_data_segment_type_id_list.append(after_billing_segment_billing["data_segment_type_id"])
                write_data_category_id_list.append(after_billing_segment_billing["data_category_id"])
            else:
                write_state_list.append(None)
                write_is_public_list.append(None)
                write_data_segment_type_id_list.append(None)
                write_data_category_id_list.append(None)
            write_billing_response.append(billing_output_messages[after_billing_code])

        get_segments_current_time = time.time()
        get_segments_elapsed_secs = get_segments_current_time - get_segments_start_time
        get_segments_authentication_timeover = get_segments_elapsed_secs - AUTHENTICATION_LIMIT_SECS * get_segments_authenticate_count

        if get_segment_row_num < len(code_list):
            if batch_num % 2 == 0:
                print("Sleep 60 seconds to avoid limit")
                variables.logger.warning("{} Sleep 60 seconds to avoid limit".format(datetime.datetime.now().isoformat()))
                time.sleep(60)

            if get_segments_authentication_timeover > 0:
                authenticate()
                get_segments_authenticate_count += 1

        batch_num += 1

    write_df = pd.DataFrame({
                    "Segment ID":write_segment_id_list,
                    'code':write_code_list,
                    'Segment Name':write_segment_name_list,
                    "Segment Description":write_segment_description_list,
                    'Price':write_price_list,
                    'Duration':write_duration_list,
                    'Member ID':write_member_id_list,
                    'State':write_state_list,
                    'Is Public':write_is_public_list,
                    "Data Segment Type ID":write_data_segment_type_id_list,
                    'Data Category ID':write_data_category_id_list,
                    'Get Segment Response':write_response,
                    'Get Billing Response':write_billing_response
                })
    return write_excel.write(write_df, "DONOTUPLOAD_" + file_name + "_retrieve_segments", SHEET_NAME)


def add_segment_billing(segment_id, code, state, data_category_id, is_public, data_segment_type_id, add_billing_outputs):
    state = str(state).lower() == "active"
    is_public = str(is_public).lower() == "true"

    segment_billing_to_add = {
                                "segment_id":int(segment_id),
                                "data_provider_id":MEMBER_ID,
                                "data_category_id":int(data_category_id),
                                "active":state,
                                "is_public":is_public,
                                "data_segment_type_id":str(data_segment_type_id)
                            }
    # print(segment_billing_to_add)
    try:
        request_to_send = requests.post(url_segment_billing_category,
                                    headers={
                                        'Content-Type':'application/json',
                                        'Authorization':auth_token
                                    },
                                    params={
                                        'member_id':MEMBER_ID
                                    },
                                    json={
                                        'segment-billing-category':segment_billing_to_add
                                    })
        print("Add Segment Billing Request: {}".format(request_to_send.url))
        variables.logger.warning("{} Add Segment Billing Request: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        add_segment_billing_response = request_to_send.json()
        # print(add_segment_billing_response)
        add_billing_outputs[code] = add_segment_billing_response["response"]["status"]
    except Exception:
        try:
            add_billing_outputs[code] = add_segment_billing_response["response"]["error"]
        except:
            add_billing_outputs[code] = add_segment_billing_response

    # print("Segment ID: '{}' added segment billing".format(segment_id))

def edit_segment_billing(segment_billing_id, segment_id, code, state, data_category_id, is_public, data_segment_type_id, edit_billing_outputs):
    state = str(state).lower() == "active"
    is_public = str(is_public).lower() == "true"

    segment_billing_to_edit = {
                                "id":int(segment_billing_id),
                                "segment_id":int(segment_id),
                                "data_provider_id":MEMBER_ID,
                                "data_category_id":int(data_category_id),
                                "active":state,
                                "is_public":is_public,
                                "data_segment_type_id":str(data_segment_type_id)
                            }
    # print("Edit Billing Json request")
    # print(segment_billing_to_edit)
    try:
        request_to_send = requests.put(url_segment_billing_category,
                                    headers={
                                        'Content-Type':'application/json',
                                        'Authorization':auth_token
                                    },
                                    params={
                                        'member_id':MEMBER_ID
                                    },
                                    json={
                                        'segment-billing-category':segment_billing_to_edit
                                    })
        print("Edit Segment Billing Request: {}".format(request_to_send.url))
        variables.logger.warning("{} Edit Segment Billing Request: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        edit_segment_billing_response = request_to_send.json()
        # print(edit_segment_billing_response)
        edit_billing_outputs[code] = edit_segment_billing_response["response"]["status"]
    except Exception:
        try:
            edit_billing_outputs[code] = edit_segment_billing_response["response"]["error"]
        except:
            edit_billing_outputs[code] = edit_segment_billing_response

def get_segment_billing(segment_id, segment_code, current_segment_billings, billing_output_messages):
    try:
    # print("Segment ID: {}".format(segment_id))
        request_to_send = requests.get(url_segment_billing_category,
                                    headers={
                                        'Content-Type':'application/json',
                                        'Authorization':auth_token
                                    },
                                    params={
                                        'segment_id':segment_id
                                    })
        print("Get Segment Billing Request: {}".format(request_to_send.url))
        variables.logger.warning("{} Get Segment Billing Request: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))
        get_segment_billing_response = request_to_send.json()
        # print(get_segment_billing_response)
        segment_billing_category = get_segment_billing_response["response"]["segment-billing-categories"][0]
        
        billing_id = segment_billing_category["id"]
        bool_state = segment_billing_category["active"]
        state = "active"
        if not bool_state:
            state = "inactive"
        # print("State: {}".format(state))

        is_public = segment_billing_category["is_public"]
        # print("Is Public: {}".format(is_public))
        data_segment_type_id = segment_billing_category["data_segment_type_id"]
        # print("Data Segment Type ID: {}".format(data_segment_type_id))
        data_category_id = segment_billing_category["data_category_id"]
        # print("Data Category ID: {}".format(data_category_id))
        current_segment_billings[segment_code] = {
                                                    "id":billing_id,
                                                    "state":state,
                                                    "is_public":is_public,
                                                    "data_segment_type_id":data_segment_type_id,
                                                    "data_category_id":data_category_id
                                                }
        billing_output_messages[segment_code] = get_segment_billing_response["response"]["status"]
    except:
        current_segment_billings[segment_code] = None
        # print("No Segment Billing")
        # print(get_segment_billing_response)
        try:
            billing_output_messages[segment_code] = get_segment_billing_response["response"]['error']
        except:
            billing_output_messages[segment_code] = get_segment_billing_response

def get_segment_billing_range(start_element, element_num, segment_billing_dict, total_elements, to_get_total_elements, counter_to_wait):
    if counter_to_wait % 4 == 0:
        print("Sleep for 20 seconds to avoid call limit")
        variables.logger.warning("{} Sleep for 20 seconds to avoid call limit".format(datetime.datetime.now().isoformat()))
        time.sleep(20)
    
    request_to_send = requests.get(url_segment_billing_category,
                                headers={
                                    'Content-Type':'application/json',
                                    'Authorization':auth_token
                                },
                                params={
                                    "start_element":start_element,
                                    "num_elements":RETRIEVE_SEGMENTS_NUM_ELEMENTS
                                })
    print("Get All Segment Billing URL: {}".format(request_to_send.url))
    variables.logger.warning("{} Get All Segment Billing URL: {}".format(datetime.datetime.now().isoformat(), request_to_send.url))

    start_element += element_num
    response_json = request_to_send.json()
    response = response_json["response"]
    
    try:
        if to_get_total_elements:
            total_elements[0] = response["count"]
            # print("Total elements: {}".format(total_elements))
            to_get_total_elements = False

        segment_billing_categories = response["segment-billing-categories"]

        for segment_billing_category in segment_billing_categories:
            segment_billing_category_id = segment_billing_category["id"]
            segment_id = segment_billing_category["segment_id"]
            data_provider_id = segment_billing_category["data_provider_id"]
            data_category_id = segment_billing_category["data_category_id"]
            active = segment_billing_category["active"]
            member_id = segment_billing_category["member_id"]
            is_public = segment_billing_category["is_public"]
            recommend_include = segment_billing_category["recommend_include"]
            data_segment_type_id = segment_billing_category["data_segment_type_id"]

            segment_billing_dict[segment_id] = {
                                                    "is_public":is_public,
                                                    "active":active,
                                                    "data_segment_type_id":data_segment_type_id,
                                                    "data_category_id":data_category_id
                                                }
    except:
        print("ERROR: {}".format(response["error"]))
        variables.logger.warning("{} Error: {}".format(datetime.datetime.now().isoformat(), response["error"]))
    finally:
        print("Sleep for 20 seconds to avoid call limit")
        variables.logger.warning("{} Sleep for 20 seconds to avoid call limit".format(datetime.datetime.now().isoformat()))
        time.sleep(20)

def get_all_segment_billing():
    total_elements = [1] 
    to_get_total_elements = True
    segment_billing_dict = {}
    threads = []

    global appnexus_pool
    start_element = 0
    counter_to_wait = 1
    while start_element <= total_elements[0]:
        thread_counter = 0

        while thread_counter < THREAD_LIMIT and start_element <= total_elements[0]:
            appnexus_pool.add_task(get_segment_billing_range, start_element, RETRIEVE_SEGMENTS_NUM_ELEMENTS, segment_billing_dict, total_elements, to_get_total_elements, counter_to_wait)
            start_element += RETRIEVE_SEGMENTS_NUM_ELEMENTS
            thread_counter += 1
            counter_to_wait += 1

        appnexus_pool.wait_completion()

    return segment_billing_dict

# End of Segment Billing Functions