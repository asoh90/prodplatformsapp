import requests
import write_excel
import variables
import pandas as pd

# API URL
URL_HOME = "https://api.thetradedesk.com/v3/"
URL_AUTHENTICATION = URL_HOME + "authentication"
URL_CREATE_OR_EDIT = URL_HOME + "thirdpartydata"
URL_QUERY = URL_HOME + "thirdpartydata/query"

SHEET_NAME = "TTD"

# Folder to retrieve uploaded file
UPLOAD_FOLDER = variables.UPLOAD_FOLDER

# Login credentials
login = None
password = None

# Provider ID
PROVIDER_ID = "eyeota"

# Parent Provider ID to ignore (i.e. not append to child name)
TEMP_PROVIDER_ID_TO_IGNORE = ['', '1', 'ROOT', 'None']


# callAPI function will decide what function in ttd to call. platform_manager.py will call this function 
# if platform selected is "The Trade Desk"
def callAPI(function, file_path):
    # Login credentials
    try:
        global login; login = variables.login_credentials['TTD']['Login']
        global password; password = variables.login_credentials['TTD']['PW']
    except:
        return {"message":"ERROR: Incorrect login credentials! Please download 'asoh-flask-deploy.sh' file from <a href='https://eyeota.atlassian.net/wiki/pages/viewpageattachments.action?pageId=127336529&metadataLink=true'>Confluence</a> again!>"}

    output = "ERROR: option is not available"
    if (function == "Query All Segments"):
        query_output = get_query_all()
        # Error retrieving query
        if ("message" in query_output):
            return query_output
        
        output = processJsonOutput(query_output, "query")
    else:
        if (function == "Add Segments"):
            function = "add"
        elif (function == "Edit Segments"):
            function = "edit"
        add_edit_output = read_file(file_path, function)

        output = process_add_edit_output(add_edit_output, function)
    return output

# get authentication code. return None if credentials fail
def getAuthenticationCode():
    auth_code = None
    auth_data = requests.post(URL_AUTHENTICATION,
                    headers={
                        'Content-Type':'application/json'
                    },
                    json={
                        'Login':login,
                        'Password':password,
                        'TokenExpirationInMinutes':3600
                    }).json()

    # auth_code is null if credentials are incorrect
    try:
        auth_code = auth_data["Token"]
        return auth_code
    except:
        return auth_code

# query all the third party data in Trade Desk system
def get_query_all():
    auth_code = getAuthenticationCode()
    if (auth_code == None):
        return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

    query_data = requests.post(URL_QUERY,
                    headers={
                        'Content-Type':'application/json',
                        'TTD-Auth': auth_code
                    },
                    json={
                        'ProviderId':PROVIDER_ID,
                        'PageStartIndex':0,
                        'PageSize':None
                    }).json()
    print("Query Request: " + URL_QUERY)
    # write to file
    return query_data

def read_file(file_path, function):
    read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])

    segment_id_list = read_df['Segment ID']
    parent_segment_id_list = read_df['Parent Segment ID']
    segment_name_list = read_df['Segment Name']
    segment_description_list = read_df['Segment Description']
    buyable_list = read_df['Buyable']

    output = None
    result_list = []
    try:
        for row_num in segment_id_list.index:
            provider_element_id = segment_id_list[row_num]
            parent_element_id = str(parent_segment_id_list[row_num])
            display_name = segment_name_list[row_num]
            buyable = buyable_list[row_num]
            if (buyable.lower == "buyable"):
                buyable = True
            else:
                buyable = False
            description = segment_description_list[row_num]
            
            # to do - call for add or edit function based on the function
            output = add_or_edit(provider_element_id, parent_element_id, display_name, buyable, description, function)
            result_list.append(output)
    except Exception as e:
        print("ERROR: " + e)
    finally: 
        json_output = {'Result':result_list}
        return json_output

# Add function returns a json format for each call, to be appended to the results before processJsonOutput
def add_or_edit(provider_element_id, parent_element_id, display_name, buyable, description, function):
    auth_code = getAuthenticationCode()
    output_json_data = None
    if (auth_code == None):
        return{'message':"ERROR: getting TTD Auth Code. Please check <b>ttd.py</b> if credentials are correct."}

    json_to_send = {
                        'ProviderId':PROVIDER_ID,
                        'ProviderElementId':provider_element_id,
                        'ParentElementId':parent_element_id,
                        'DisplayName':display_name,
                        'Buyable':buyable,
                        'Description':description
                    }
    if (function == "add"):
        output_raw_data = requests.post(URL_CREATE_OR_EDIT,
                        headers={
                            'Content-Type':'application/json',
                            'TTD-Auth': auth_code
                        },
                        json=json_to_send)
        # print(output_raw_data)
        output_json_data = output_raw_data.json()
    elif (function == "edit"):
        output_data = requests.put(URL_CREATE_OR_EDIT,
                        headers={
                            'Content-Type':'application/json',
                            'TTD-Auth': auth_code
                        },
                        json=json_to_send)
        # print(output_raw_data.body)
        output_json_data = output_raw_data.json()
    
    result = "OK"
    if ("Message" in output_json_data):
        result = output_json_data["Message"]
        print("result: {}".format(result))
    return {"message":result, "output":json_to_send}

def store_segment_in_dict(json_output):
    segment_dictionary = {}

    # Load all the segments into a dictionary to formulate full segment name later
    for row in json_output['Result']:
        provider_element_id = str(row['ProviderElementId'])
        parent_element_id = str(row['ParentElementId'])
        display_name = str(row['DisplayName'])

        segment_dictionary[provider_element_id] = {"display_name":display_name,"parent_element_id":parent_element_id}
    return segment_dictionary

def get_full_segment_name(parent_segment_id, child_segment_name, segment_dictionary):
    while parent_segment_id in segment_dictionary and not parent_segment_id in TEMP_PROVIDER_ID_TO_IGNORE:
        child_segment_name = segment_dictionary[parent_segment_id]["display_name"] + " - " + child_segment_name
        parent_segment_id = segment_dictionary[parent_segment_id]["parent_element_id"]
    return child_segment_name
        
# based on the output from TTD API, format them into json format to write to file
def processJsonOutput(json_output, function):
    # try:
    write_provider_id = []
    write_provider_element_id = []
    write_parent_element_id = []
    write_display_name = []
    write_buyable = []
    write_description = []
    write_audience_size = []

    segment_dictionary = store_segment_in_dict(json_output)

    # Print results
    for row in json_output['Result']:
        provider_id = str(row['ProviderId'])
        provider_element_id = str(row['ProviderElementId'])
        parent_element_id = str(row['ParentElementId'])
        display_name = str(row['DisplayName'])
        buyable = row['Buyable']
        description = str(row['Description'])
        audience_size = str(row['AudienceSize'])

        # loop to get full segment name
        display_name = get_full_segment_name(parent_element_id, display_name, segment_dictionary)

        write_provider_id.append(provider_id)
        write_provider_element_id.append(provider_element_id)
        write_parent_element_id.append(parent_element_id)
        write_display_name.append(display_name)
        write_buyable.append(buyable)
        write_description.append(description)
        write_audience_size.append(audience_size)

    write_df = pd.DataFrame({
                                "Provider ID":write_provider_id,
                                "Segment ID":write_provider_element_id,
                                "Parent Segment ID":write_parent_element_id,
                                "Segment Name":write_display_name,
                                "Buyable":write_buyable,
                                "Description":write_description,
                                "Audience Size":write_audience_size
                            })
    return write_excel.write_and_email(write_df, "DONOTUPLOAD_The_Trade_Desk_" + function, SHEET_NAME)
    # return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_" + function, SHEET_NAME)
    # except:
    #     return {"message":"ERROR Processing TTD Json File for Query All Segments"}

def process_add_edit_output(output_list, function):
    # try:
        write_provider_id = []
        write_provider_element_id = []
        write_parent_element_id = []
        write_display_name = []
        write_buyable = []
        write_description = []
        write_audience_size = []
        write_result = []

        query_output = get_query_all()
        segment_dictionary = store_segment_in_dict(query_output)

        for row in output_list['Result']:
            output_row = row["output"]
            provider_id = str(output_row['ProviderId'])
            provider_element_id = str(output_row['ProviderElementId'])
            parent_element_id = str(output_row['ParentElementId'])
            display_name = str(output_row['DisplayName'])
            buyable = output_row['Buyable']
            description = str(output_row['Description'])
            message = str(row["message"])

            # loop to get full segment name
            if message == "OK":
                display_name = get_full_segment_name(parent_element_id, display_name, segment_dictionary)

            write_provider_id.append(provider_id)
            write_provider_element_id.append(provider_element_id)
            write_parent_element_id.append(parent_element_id)
            write_display_name.append(display_name)
            write_buyable.append(buyable)
            write_description.append(description)
            write_result.append(message)

        write_df = pd.DataFrame({
                                    "Provider ID":write_provider_id,
                                    "Segment ID":write_provider_element_id,
                                    "Parent Segment ID":write_parent_element_id,
                                    "Segment Name":write_display_name,
                                    "Buyable":write_buyable,
                                    "Description":write_description,
                                    function + " Result": write_result
                                })
        return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_" + function, SHEET_NAME)
    # except:
    #     return {"message":"ERROR Processing TTD Json File for Add/Edit Segments"}