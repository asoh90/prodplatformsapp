import requests
import write_excel
import variables
import pandas as pd
import datetime

# API URL
URL_HOME = "https://api.thetradedesk.com/v3/"
URL_AUTHENTICATION = URL_HOME + "authentication"
URL_CREATE_OR_EDIT = URL_HOME + "thirdpartydata"
URL_QUERY = URL_HOME + "thirdpartydata/query"
URL_DATARATE = URL_HOME + "/datarate"
URL_DATARATE_BATCH = URL_DATARATE + "/batch"
URL_DATARATE_QUERY = URL_DATARATE + "/query"

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
    if function == "Query All Segments":
        auth_code = authenticate()
        if (auth_code == None):
            return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

        query_output = get_query_all(auth_code)
        # Error retrieving query
        if "auth_error" in query_output:
            return query_output
        
        output = processJsonOutput(query_output, "query")
    elif function == "Edit Custom Segment Rates":
        output = read_file_to_edit_custom_segment_rates(file_path)
    elif function == "Retrieve Partner Rates":
        output = read_file_to_retrieve_partner_rates(file_path)
    elif function == "Retrieve Batch Status":
        output = read_file_to_retrieve_batch_id_status(file_path)
    else:
        if function == "Add Custom Segments":
            function = 'Add'
        elif function == "Edit Custom Segments":
            function = 'Edit'
        output = read_file_to_add_or_edit_custom_segments(file_path, function)

    return output

# get authentication code. return None if credentials fail
def authenticate():
    auth_code = None
    auth_request = requests.post(URL_AUTHENTICATION,
                    headers={
                        'Content-Type':'application/json'
                    },
                    json={
                        'Login':login,
                        'Password':password,
                        'TokenExpirationInMinutes':3600
                    })
    print("Authentication Request: {}".format(auth_request.url))
    variables.logger.warning("{} Authentication Request: {}".format(datetime.datetime.now().isoformat(), auth_request.url))
    auth_data = auth_request.json()
    # auth_code is null if credentials are incorrect
    try:
        auth_code = auth_data["Token"]
        return auth_code
    except:
        return auth_code

# query all the third party data in Trade Desk system
def get_query_all(auth_code):
    post_query = requests.post(URL_QUERY,
                    headers={
                        'Content-Type':'application/json',
                        'TTD-Auth': auth_code
                    },
                    json={
                        'ProviderId':PROVIDER_ID,
                        'PageStartIndex':0,
                        'PageSize':None
                    })
    print("Query Request: {}".format(post_query.url))
    variables.logger.warning("{} Query Request: {}".format(datetime.datetime.now().isoformat(), post_query.url))

    query_data = post_query.json()
    return query_data

def retrieve_batch_id_status(auth_code, batch_id):
    output_raw_data = requests.post(URL_DATARATE_QUERY,
                            params={
                                "batchId":batch_id
                            },
                            headers={
                                'Content-Type':'application/json',
                                'TTD-Auth': auth_code
                            })
    print("Get Batch Status URL: {}".format(output_raw_data.url))
    variables.logger.warning("{} Query Request: {}".format(datetime.datetime.now().isoformat(), output_raw_data.url))

    output_data = output_raw_data.json()
    if output_raw_data.status_code == 200:
        return output_data
    else:
        return {'api_error':output_data}


def read_file_to_retrieve_batch_id_status(file_path):
    read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
    batch_id_checked = {}

    write_segment_id_list = []
    batch_id_list = read_df["Batch ID"]
    write_seat_id_list = []
    write_price_id_list = []
    write_currency_list = []
    write_processing_status_list = []
    write_approval_status_list = []
    write_error_list = []

    auth_code = authenticate()
    if (auth_code == None):
        return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

    for batch_id in batch_id_list:
        if not batch_id in batch_id_checked:
            batch_id_status_output = retrieve_batch_id_status(auth_code, batch_id)

            if "api_error" in batch_id_status_output:
                write_segment_id_list.append(None)
                write_seat_id_list.append(None)
                write_price_list.append(None)
                write_currency_list.append(None)
                write_processing_status_list.append(None)
                write_approval_status_list.append(None)
                write_error_list.append(batch_id_status_output["api_error"])
            else:
                processing_status = batch_id_status_output["ProcessingStatus"]
                approval_status = batch_id_status_output["ApprovalStatus"]

                data_rates_output = batch_id_status_output["DataRates"]
                for data_rate_output in data_rates_output:
                    write_segment_id_list.append(data_rate_output["ProviderElementId"])
                    write_seat_id_list.append(data_rate_output["PartnerId"])
                    write_price_list.append(float(data_rate_output["CPMRate"]["Amount"]))
                    write_currency_list.append(float(data_rate_output["CPMRate"]["Currency"]))
                    write_processing_status_list.append(processing_status)
                    write_approval_status_list.append(approval_status)
                    write_error_list.append(None)

            batch_id_checked[batch_id] = None

    write_df = pd.DataFrame({
                                "Segment ID":write_segment_id_list,
                                "Brand": brand_list,
                                "Seat ID": write_seat_id_list,
                                "Price": write_price_list,
                                "Currency": write_currency_list,
                                "Processing Status": write_processing_status_list,
                                "Approval Status": write_approval_status_list,
                                "Error": write_error_list
                            })

    return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_BatchId_Status", SHEET_NAME)

def read_file_to_add_or_edit_custom_segments(file_path, function):
    read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])

    rates_created_list = {}

    segment_id_list = read_df['Segment ID']
    parent_segment_id_list = read_df['Parent Segment ID']
    segment_name_list = read_df['Segment Name']
    segment_description_list = read_df['Segment Description']
    buyable_list = read_df['Buyable']
    brand_list = read_df['Brand']
    seat_id_list = read_df['Seat ID']
    price_list = read_df['Price']
    output_list = []
    rates_output_list = []

    auth_code = authenticate()
    if (auth_code == None):
        return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

    row_num = 0
    for segment_id in segment_id_list:
        parent_segment_id = parent_segment_id_list[row_num]
        segment_name = segment_name_list[row_num]
        segment_description = segment_name_list[row_num]
        buyable = buyable_list[row_num]
        brand = brand_list[row_num]
        seat_id = seat_id_list[row_num]
        price = price_list[row_num]

        output = add_or_edit(auth_code, segment_id, parent_segment_id, segment_name, buyable, segment_description, function)
        if "auth_error" in output:
            return output
        output_list.append(output)

        rate_output = None
        # segments right below the root custom segment should add rate
        if parent_segment_id == "bumcust" or parent_segment_id == "eyecustomseg" or parent_segment_id == "ttdratetest_partnerID_rate":
            rate_output = add_rate(auth_code, brand, segment_id, seat_id, price)
            rates_created_list[segment_id] = None
        # if parent segment is not bumcust, eyecustomseg, or ttdratetest_partnerID_rate, add the segment rate
        elif parent_segment_id not in rates_created_list:
            rate_output = add_rate(auth_code, brand, segment_id, seat_id, price)
            rates_created_list[segment_id] = None

        if rate_output is None:
            rates_output_list.append(None)
        elif "api_error" in rate_output:
            rates_output_list.append(rate_output["api_error"])
        else:
            rates_output_list.append(rate_output)

        row_num += 1

    write_df = pd.DataFrame({
                                "Segment ID":segment_id_list,
                                "Parent Segment ID": parent_segment_id_list,
                                "Segment Name": segment_name_list,
                                "Buyable": buyable_list,
                                "Description": segment_description_list,
                                "Brand": brand_list,
                                "Seat ID": seat_id_list,
                                "Price": price_list,
                                "Output": output_list,
                                "Rates Output": rates_output_list
                            })

    return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_" + function, SHEET_NAME)

# Add function returns a json format for each call, to be appended to the results before processJsonOutput
def add_or_edit(auth_code, provider_element_id, parent_element_id, display_name, buyable, description, function):
    output_raw_data = None

    if not buyable:
        buyable = False
    else:
        buyable = True

    json_to_send = {
                        'ProviderId':PROVIDER_ID,
                        'ProviderElementId':str(provider_element_id),
                        'ParentElementId':str(parent_element_id),
                        'DisplayName':str(display_name),
                        'Buyable':buyable,
                        'Description':str(description)
                    }
    
    try:
        if function == "Add":
            output_raw_data = requests.post(URL_CREATE_OR_EDIT,
                            headers={
                                'Content-Type':'application/json',
                                'TTD-Auth': auth_code
                            },
                            json=json_to_send)
            print("Add Custom Segments URL: {}".format(output_raw_data.url))
        elif function == "Edit":
            output_raw_data = requests.put(URL_CREATE_OR_EDIT,
                            headers={
                                'Content-Type':'application/json',
                                'TTD-Auth': auth_code
                            },
                            json=json_to_send)
            print("Edit Custom Segments URL: {}".format(output_raw_data.url))
        output_json_data = output_raw_data.json()

        if (output_raw_data.status_code == 200):
            result = "OK"
        else:
            result = output_json_data["Message"]

        return result
    except:
        print("Unidentified error adding or editing segment for TTD")
        variables.logger.warning("Unidentified error adding or editing segment for TTD")
        return {"api_error":"Unidentified error adding or editing segment"}

def retrieve_partner_rates(auth_code, brand, partner_id):
    output_raw_data = None
    
    if brand.lower() == "bombora":
        brand = "eye87bom"
    elif brand.lower() == "eyeota":
        brand = "fz867ve"
    else:
        return {"api_error": "Invalid value for Brand. Valid values: 'eyeota' or 'bombora'."}
    
    json_to_send = {
        "ProviderID":PROVIDER_ID,
        "BrandID":brand,
        "RateLevel":"Partner",
        "PartnerID":str(partner_id),
        "PageSize":None,
        "PageStartIndex":0
    }

    try:
        output_raw_data = requests.post(URL_DATARATE_QUERY,
                            headers={
                                'Content-Type': 'application/json',
                                'TTD-Auth': auth_code
                            },
                            json=json_to_send)
        print("Retrieve Data Rate URL: {}".format(output_raw_data.url))
        return output_raw_data.json()
    except:
        print("Unidentified error retrieving partner data rates")
        variables.logger.warning("Unidentified error retrieving partner data rates")
        return {"api_error": "Unidentified error retrieving partner data rates"}

def read_file_to_retrieve_partner_rates(file_path):
    read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])

    brand_seat_id_dict = {}

    brand_list = read_df["Brand"]
    seat_id_list = read_df["Seat ID"]

    write_brand_list = []
    write_seat_id_list = []
    write_segment_id_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_buyable_list = []
    write_cpm_list = []
    write_currency_list = []
    write_output_list = []

    auth_code = authenticate()
    if (auth_code == None):
        return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

    segment_json = get_query_all(auth_code)
    segment_dict = store_segment_in_dict(segment_json)
    segment_formatted_dictionary = {}

    for row in segment_json['Result']:
        provider_id = str(row['ProviderId'])
        provider_element_id = str(row['ProviderElementId'])
        parent_element_id = str(row['ParentElementId'])
        display_name = str(row['DisplayName'])
        buyable = row['Buyable']
        description = str(row['Description'])
        audience_size = str(row['AudienceSize'])

        # loop to get full segment name
        display_name = get_full_segment_name(parent_element_id, display_name, segment_dict)
        
        segment_formatted_dictionary[provider_element_id] = {
            "name":display_name,
            "buyable":buyable,
            "description":description
        }

    row_num = 0
    for brand in brand_list:
        seat_id = seat_id_list[row_num]

        if not brand in brand_seat_id_dict:
            brand_seat_id_dict[brand] = {}

        seat_id_dict = brand_seat_id_dict[brand]

        if not seat_id in seat_id_dict:
            seat_id_dict[seat_id] = None

            partner_rates_output = retrieve_partner_rates(auth_code, brand, seat_id)

            if "api_error" in partner_rates_output:
                write_brand_list.append(brand)
                write_seat_id_list.append(seat_id)
                write_segment_id_list.append(None)
                write_segment_name_list.append(None)
                write_segment_description_list.append(None)
                write_segment_buyable_list.append(None)
                write_cpm_list.append(None)
                write_currency_list.append(None)
                write_output_list.append(partner_rates_output["error"])
            else:
                for partner_rate in partner_rates_output["Result"]:
                    write_brand_list.append(brand)
                    write_seat_id_list.append(seat_id)

                    current_segment_id = partner_rate["ProviderElementId"]
                    current_segment = segment_formatted_dictionary[current_segment_id]
                    write_segment_id_list.append(current_segment_id)
                    write_segment_name_list.append(current_segment["name"])
                    write_segment_description_list.append(current_segment["description"])
                    write_buyable_list.append(current_segment["buyable"])
                    cpm_rate = partner_rate["CPMRate"]
                    write_cpm_list.append(float(cpm_rate["Amount"]))
                    write_currency_list.append(cpm_rate["CurrencyCode"])
                    write_output_list.append(None)
        row_num += 1

    write_df = pd.DataFrame({
                                "Segment ID": write_segment_id_list,
                                "Segment Name": write_segment_name_list,
                                "Segment Description": write_segment_description_list,
                                "Buyable": write_buyable_list,
                                "Brand":write_brand_list,
                                "Seat ID": write_seat_id_list,
                                "Price": write_cpm_list,
                                "Currency": write_currency_list,
                                "Output": write_output_list
                            })

    return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_Partner_Rates", SHEET_NAME)

def add_rate(auth_code, brand, provider_element_id, partner_id, price):
    output_raw_data = None
    
    if brand.lower() == "bombora":
        brand = "eye87bom"
    elif brand.lower() == "eyeota":
        brand = "fz867ve"
    else:
        return {"api_error": "Invalid value for Brand. Valid values: 'eyeota' or 'bombora'."}

    json_to_send = {
                        "ProviderID":PROVIDER_ID,
                        "DataRates":[{
                            "ProviderElementID":str(provider_element_id),
                            "BrandID":brand,
                            "RateLevel": "Partner",
                            "PartnerID":str(partner_id), # This is the seat ID
                            "RateType":"CPM",
                            "CPMRate": {
                                "Amount":float(price),
                                "CurrencyCode":"USD"
                            }
                        }]
                    }

    try:
        output_raw_data = requests.post(URL_DATARATE_BATCH,
                        headers={
                            'Content-Type': 'application/json',
                            'TTD-Auth': auth_code
                        },
                        json=json_to_send)
        print("Add Data Rate URL: {}".format(output_raw_data.url))

        output_json_data = output_raw_data.json()
        if output_raw_data.status_code == 200:
            return output_json_data["BatchId"]
        else:
            return output_json_data

    except:
        print("Unidentified error with Rates API")
        variables.logger.warning("Unidentified error with Rates API")
        return {"api_error":"Unidentified error with Rates API"}

def read_file_to_edit_custom_segment_rates(file_path):
    read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])

    segment_id_list = read_df["Segment ID"]
    brand_list = read_df["Brand"]
    seat_id_list = read_df["Seat ID"]
    price_list = read_df["Price"]
    write_output_list = []

    row_num = 0
    for segment_id in segment_id_list:
        brand = brand_list[row_num]
        seat_id = seat_id_list[row_num]
        price = price_list[row_num]

        rate_output = add_rate(auth_code, brand, segment_id, seat_id, price)
        
        if "api_error" in rate_output:
            write_output_list.append(rate_output["api_error"])
        else:
            write_output_list.append(rate_output)
        row_num += 1

    write_df = pd.DataFrame({
                                "Segment ID": segment_id_list,
                                "Brand":brand_list,
                                "Seat ID": seat_id_list,
                                "Price": price_list,
                                "Output": write_output_list
                            })

    return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_Edit_Rates", SHEET_NAME)

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