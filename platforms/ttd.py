import requests
import write_excel
import variables
import pandas as pd
import datetime
import time

# API URL
URL_HOME = "https://api.thetradedesk.com/v3/"
URL_AUTHENTICATION = URL_HOME + "authentication"
URL_CREATE_OR_EDIT = URL_HOME + "thirdpartydata"
URL_QUERY = URL_HOME + "thirdpartydata/query"
URL_DATARATE = URL_HOME + "datarate"
URL_DATARATE_BATCH = URL_DATARATE + "/batch"
URL_DATARATE_QUERY = URL_DATARATE + "/query"

SHEET_NAME = "TTD"

# Folder to retrieve uploaded file
UPLOAD_FOLDER = variables.UPLOAD_FOLDER

# Login credentials
login = None
password = None

# Fixed values
PROVIDER_ID = "eyeota"
BOMBORA_BRAND_ID = "eye87bom"
EYEOTA_BRAND_ID = "fz867ve"
RATES_PAGE_SIZE = 100
PUBLIC_TAXO_RATE_LEVEL = "System"
PARTNER_TAXO_RATE_LEVEL = "Partner"
ADVERTISER_TAXO_RATE_LEVEL = "Advertiser"

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

    start_time = time.time()

    output = "ERROR: option is not available"
    if function == "Query All Segments":
        auth_code = authenticate()
        if (auth_code == None):
            return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

        query_output = get_query_all(auth_code)
        # Error retrieving query
        if "auth_error" in query_output:
            return query_output
        
        output = processJsonOutput(auth_code, query_output, "query")
    else:
        # Check if SHEET_NAME exists in uploaded file
        try:
            read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])
        except:
            return{'message':"ERROR: Unable to find sheet name: {}".format(SHEET_NAME)}

        elif function == "Edit Segment Rates":
            output = read_file_to_edit_segment_rates(file_path)
        elif function == "Retrieve Custom Segments":
            output = read_file_to_retrieve_custom_segments(file_path)
        elif function == "Retrieve Batch Status":
            output = read_file_to_retrieve_batch_id_status(file_path)
        else:
            if function == "Add Segments":
                function = 'Add'
            elif function == "Edit Segments":
                function = 'Edit'
            output = read_file_to_add_or_edit_segments(file_path, function)

    elapsed_time = time.time() - start_time
    elapsed_mins = int(elapsed_time/60)
    elapsed_secs = int(elapsed_time%60)

    print("Elapsed time: {} mins {} secs".format(elapsed_mins, elapsed_secs))

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

def append_rates_to_push(brand, provider_element_id, partner_id, advertiser_id, price, price_type, rates_to_push_list):
    output_raw_data = None
    
    try:
        if brand.lower() == "bombora":
            brand = BOMBORA_BRAND_ID
        elif brand.lower() == "eyeota":
            brand = EYEOTA_BRAND_ID
        # else:
        #     return rates_to_push_list,{"api_error": "Invalid value for Brand. Valid values: 'eyeota' or 'bombora'."}
    except:
        return rates_to_push_list, {"api_error":"Brand cannot be changed to lowercase"}

    temp_rates_to_push = {
                            "ProviderElementID":str(provider_element_id),
                            "BrandID":brand,
                        }
    
    try:
        numpy.isnan(price_type)
        return rates_to_push_list, {"api_error":"Invalid Price Type. Only 'CPM' and 'PercentOfMediaCost' allowed"}
    except:
        if price_type.lower() == "cpm":
            temp_rates_to_push["RateType"] = "CPM"
            temp_rates_to_push["CPMRate"] = {
                                    "Amount":float(price),
                                    "CurrencyCode":"USD"
                                }
        else:
            temp_rates_to_push["RateType"] = "PercentOfMediaCost"
            temp_rates_to_push["PercentOfMediaCostRate"] = float(price)

    try:
        numpy.isnan(advertiser_id)
        numpy.isnan(partner_id)
        temp_rates_to_push["RateLevel"] = PUBLIC_TAXO_RATE_LEVEL
    except:
        try:
            numpy.isnan(advertiser_id)
            temp_rates_to_push["PartnerID"] = str(partner_id)
            temp_rates_to_push["RateLevel"] = PARTNER_TAXO_RATE_LEVEL
        except:
            temp_rates_to_push["AdvertiserID"] = str(advertiser_id)
            temp_rates_to_push["RateLevel"] = ADVERTISER_TAXO_RATE_LEVEL

    rates_to_push_list.append(temp_rates_to_push)

    return rates_to_push_list,"OK"

def retrieve_batch_id_status(auth_code, batch_id):
    output_raw_data = requests.get(URL_DATARATE_BATCH + "/" + batch_id,
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
    write_segment_name_list = []
    write_segment_description_list = []
    batch_id_list = read_df["Batch ID"]
    write_brand_list = []
    write_partner_id_list = []
    write_advertiser_id_list = []
    write_price_list = []
    write_currency_list = []
    write_processing_status_list = []
    write_approval_status_list = []
    write_error_list = []
    write_batch_id_list = []

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

    for batch_id in batch_id_list:
        if not batch_id in batch_id_checked:
            batch_id_status_output = retrieve_batch_id_status(auth_code, batch_id)

            if "api_error" in batch_id_status_output:
                write_segment_id_list.append(None)
                write_segment_name_list.append(None)
                write_segment_description_list.append(None)
                write_brand_list.append(None)
                write_partner_id_list.append(None)
                write_advertiser_id_list.append(None)
                write_price_list.append(None)
                write_currency_list.append(None)
                write_batch_id_list.append(None)
                write_processing_status_list.append(None)
                write_approval_status_list.append(None)
                write_error_list.append(batch_id_status_output["api_error"])
            else:
                processing_status = batch_id_status_output["ProcessingStatus"]
                approval_status = batch_id_status_output["ApprovalStatus"]

                data_rates_output = batch_id_status_output["DataRates"]
                for data_rate_output in data_rates_output:
                    segment_id = data_rate_output["ProviderElementId"]
                    write_segment_id_list.append(segment_id)
                    segment = segment_formatted_dictionary[segment_id]
                    write_segment_name_list.append(segment["name"])
                    write_segment_description_list.append(segment["description"])

                    brand_id = data_rate_output["BrandId"]
                    if brand_id == BOMBORA_BRAND_ID:
                        brand_id = "bombora"
                    elif brand_id == EYEOTA_BRAND_ID:
                        brand_id = "eyeota"

                    write_brand_list.append(brand_id)
                    write_seat_id_list.append(data_rate_output["PartnerId"])
                    write_price_list.append(float(data_rate_output["CPMRate"]["Amount"]))
                    write_currency_list.append(data_rate_output["CPMRate"]["CurrencyCode"])
                    write_batch_id_list.append(batch_id)
                    write_processing_status_list.append(processing_status)
                    write_approval_status_list.append(approval_status)
                    write_error_list.append(None)

            batch_id_checked[batch_id] = None

    write_df = pd.DataFrame({
                                "Segment ID":write_segment_id_list,
                                "Segment Name": write_segment_name_list,
                                "Segment_Description": write_segment_description_list,
                                "Brand": write_brand_list,
                                "Partner ID": write_partner_id_list,
                                "Advertiser ID": write_advertiser_id_list,
                                "Price": write_price_list,
                                "Currency": write_currency_list,
                                "Batch ID": write_batch_id_list,
                                "Processing Status": write_processing_status_list,
                                "Approval Status": write_approval_status_list,
                                "Error": write_error_list
                            })

    return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_BatchId_Status", SHEET_NAME)

def read_file_to_add_or_edit_segments(file_path, function):
    file_name = file_path[7:len(file_path)-5]
    read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])

    rates_created_list = {}

    segment_id_list = read_df['Segment ID']
    parent_segment_id_list = read_df['Parent Segment ID']
    segment_name_list = read_df['Segment Name']
    segment_description_list = read_df['Segment Description']
    buyable_list = read_df['Buyable']
    brand_list = read_df['Brand']
    partner_id_list = read_df['Partner ID']
    advertiser_id_list = read_df['Advertiser ID']
    price_list = read_df['Price']
    price_type_list = read_df['Price Type']
    data_provider_list = []
    segment_full_path_list = []
    ttd_account_manager_list = []
    date_list = []
    output_list = []
    rates_output_list = []

    rates_output_dict = {}
    number_of_segments = len(segment_id_list)
    rates_to_push_list = []

    auth_code = authenticate()
    if (auth_code == None):
        return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

    segment_json = get_query_all(auth_code)
    segment_dict = store_segment_in_dict(segment_json)
    # full_path_dict = {"bumcust":"Bombora > Custom", "eyecustomseg":"Custom Segment", "464":"Branded","eyeotabranded":"Branded > Eyeota"}

    row_num = 0
    for segment_id in segment_id_list:
        parent_segment_id = parent_segment_id_list[row_num]
        try:
            parent_segment_id = str(int(parent_segment_id))
        except:
            pass
        segment_name = segment_name_list[row_num]
        segment_description = segment_name_list[row_num]
        buyable = buyable_list[row_num]
        brand = brand_list[row_num]
        partner_id = partner_id_list[row_num]
        advertiser_id = advertiser_id_list[row_num]
        price = price_list[row_num]
        price_type = price_type_list[row_num]

        # full_path_keys = full_path_dict.keys()
        segment_dict[segment_id] = {"display_name":segment_name,"parent_element_id":parent_segment_id}
        segment_dict_keys = segment_dict.keys()
        
       # Gets segment full path
        if parent_segment_id in segment_dict_keys:
            # parent_segment_path = segment_dict[parent_segment_id]
            # segment_full_path = "{} > {}".format(parent_segment_path, segment_name)
            # # Adds the current segment to the full path dict
            # full_path_dict[segment_id] = segment_full_path
            # print("Segment Full Path: {}".format(segment_full_path))
            segment_full_path = get_full_segment_name(parent_segment_id, segment_name, segment_dict)

            segment_full_path_list.append(segment_full_path)
        else:
            segment_full_path_list.append("Error!!! Parent Segment ID Not Found!!")

        data_provider_list.append("eyeota")
        ttd_account_manager_list.append(None)
        date_list.append(None)

        output = add_or_edit(auth_code, segment_id, parent_segment_id, segment_name, buyable, segment_description, function)
        if "api_error" in output:
            output = output["api_error"]
        output_list.append(output)

        rate_output = None
        # creating segment without issues (OK), create rates
        if output == "OK":
            if function == "Add":
                # segments right below the root custom segment should add rate
                if parent_segment_id == "bumcust" or parent_segment_id == "eyecustomseg" or parent_segment_id == "ttdratetest_partnerID_rate" or parent_segment_id == "464" or parent_segment_id == "eyeotabranded":
                    rates_to_push_list, rate_output = append_rates_to_push(brand, segment_id, partner_id, advertiser_id, price, price_type, rates_to_push_list)
                    rates_created_list[segment_id] = None
                    if "api_error" in rate_output:
                        rates_output_dict[row_num] = rate_output["api_error"]
                # if parent segment is not bumcust, eyecustomseg, or ttdratetest_partnerID_rate, add the segment rate
                elif parent_segment_id not in rates_created_list:
                    rates_to_push_list, rate_output = append_rates_to_push(brand, segment_id, partner_id, advertiser_id, price, price_type, rates_to_push_list)
                    rates_created_list[segment_id] = None
                    if "api_error" in rate_output:
                        rates_output_dict[row_num] = rate_output["api_error"]

        if rate_output is None:
            rates_output_dict[row_num] = None

        row_num += 1

    add_rates_output = None
    if len(rates_to_push_list) > 0:
        add_rates_output = add_rate(auth_code, rates_to_push_list)
        if "api_error" in add_rates_output:
            add_rates_output = add_rates_output["api_error"]

    loop_counter = 0
    while loop_counter < number_of_segments:
        if loop_counter in rates_output_dict:
            rates_output_list.append(rates_output_dict[loop_counter])
        else:
            rates_output_list.append(add_rates_output)
        loop_counter += 1

    write_df = pd.DataFrame({
                                "Data Provider ID": data_provider_list,
                                "Segment ID": segment_id_list,
                                "Parent Segment ID": parent_segment_id_list,
                                "Segment Name": segment_name_list,
                                "Segment Description": segment_description_list,
                                "Buyable": buyable_list,
                                "Segment Full Path": segment_full_path_list,
                                "CPM": price_list,
                                "Partner ID": partner_id_list,
                                "Advertiser ID": advertiser_id_list,
                                "TTD Account Manager": ttd_account_manager_list,
                                "Campaign Live Date": date_list,
                                "Price Type": price_type_list,
                                "Brand": brand_list,
                                "Output": output_list,
                                "Rates Output": rates_output_list
                            })

    return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_{}_{}".format(function, file_name), SHEET_NAME)

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

def retrieve_partner_rates(auth_code, brand, partner_id, rate_level):
    output_raw_data = None
    
    if brand.lower() == "bombora":
        brand = BOMBORA_BRAND_ID
    elif brand.lower() == "eyeota":
        brand = EYEOTA_BRAND_ID
    # else:
    #     return {"api_error": "Invalid value for Brand. Valid values: 'eyeota' or 'bombora'."}
    
    json_to_send = {
        "ProviderID":PROVIDER_ID,
        "BrandID":brand,
        "RateLevel":rate_level,
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

def read_file_to_retrieve_custom_segments(file_path):
    read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])

    segment_id_list = read_df["Segment ID"]
    partner_id_list = read_df["Partner ID"]
    advertiser_id_list = read_df["Advertiser ID"]

    write_provider_id_list = []
    write_brand_list = []
    write_parent_id_list = []
    write_segment_name_list = []
    write_segment_description_list = []
    write_buyable_list = []
    write_segment_full_name_list = []
    write_cpm_list = []
    write_currency_list = []
    write_percent_of_media_list = []
    write_output_list = []

    auth_code = authenticate()
    if (auth_code == None):
        return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

    # get all segment information
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
            "provider_id":provider_id,
            "parent_id":parent_element_id,
            "segment_name":display_name,
            "segment_description":description,
            "buyable":buyable,
            "segment_full_name":display_full_name
        }

    row_num = 0
    for segment_id in segment_id_list:
        partner_id = partner_id_list[row_num]
        advertiser_id = advertiser_id_list[row_num]
        str_segment_id = str(segment_id)

        # indicate if there is already an error for output
        retrieve_output = False
        rates_dict = get_segments_rates(auth_code, partner_id, advertiser_id, segment_id)

        # check if segment details can be found
        try:
            segment_detail = segment_formatted_dictionary[str_segment_id]
            write_provider_id_list.append(segment_detail["provider_id"])
            write_parent_id_list.append(segment_detail["parent_id"])
            write_segment_name_list.append(segment_detail["segment_name"])
            write_segment_description_list.append(segment_detail["segment_description"])
            write_buyable_list.append(segment_detail["buyable"])
            write_segment_full_name_list.append(segment_detail["segment_full_name"])
        # else if segment detail cannot be found
        except:
            write_provider_id_list.append(None)
            write_parent_id_list.append(None)
            write_segment_name_list.append(None)
            write_segment_description_list.append(None)
            write_buyable_list.append(None)
            write_segment_full_name_list.append(None)
            write_output_list.append("Segment cannot be found!")
            retrieve_output = True

        # check if segment id has rates
        try:
            segment_rates = rates_dict[str_segment_id]
            write_brand_list.append(segment_rates["Brand"])
            write_cpm_list.append(segment_rates["CPM_Price"])
            write_currency_list.append(segment_rates["CPM_CurrencyCode"])
            write_percent_of_media_list.append(segment_rates["PercentOfMediaCost"])
            # only indicate message if segment can be found (no output message added yet)
            if not retrieve_output:
                write_output_list.append(None)

        # segment id does not have rates
        except:
            write_brand_list.append(None)
            write_cpm_list.append(None)
            write_currency_list.append(None)
            write_percent_of_media_list.append(None)
            # only indicate message if segment can be found (no output message added yet)
            if not retrieve_output:
                write_output_list.append("Segment Rates for Partner '{}' is not found!".format(seat_id))

        row_num += 1

    write_df = pd.DataFrame({
                                "Data Provider ID": write_provider_id_list,
                                "Segment ID": segment_id_list,
                                "Parent Segment ID": write_parent_id_list,
                                "Segment Name": write_segment_name_list,
                                "Segment Description": write_segment_description_list,
                                "Buyable": write_buyable_list,
                                "Brand":write_brand_list,
                                "Segment Full Name": write_segment_full_name_list,
                                "Partner ID": partner_id_list,
                                "Advertiser ID": advertiser_id_list,
                                "CPM Price": write_cpm_list,
                                "CPM Currency Code": write_currency_list,
                                "Percent Of Media Rate": write_percent_of_media_list,
                                "Output": write_output_list
                            })

    return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_Partner_Rates", SHEET_NAME)

def add_rate(auth_code, rates_to_push_list):
    output_raw_data = None

    json_to_send = {
                        "ProviderID":PROVIDER_ID,
                        "DataRates":rates_to_push_list
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

def read_file_to_edit_segment_rates(file_path):
    read_df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=[1])

    segment_id_list = read_df["Segment ID"]
    brand_list = read_df["Brand"]
    partner_id_list = read_df["Partner ID"]
    advertiser_id_list = read_df["Advertiser ID"]
    price_list = read_df["Price"]
    price_type_list = read_df["Price Type"]
    write_output_list = []

    output_dict = {}
    number_of_segments = len(segment_id_list)
    rates_to_push_list = []

    auth_code = authenticate()
    if (auth_code == None):
        return{'message':"ERROR: getting TTD Auth Code. Please check .sh file if credentials are correct."}

    row_num = 0
    for segment_id in segment_id_list:
        brand = brand_list[row_num]
        partner_id = partner_id_list[row_num]
        advertiser_id = advertiser_id_list[row_num]
        price = price_list[row_num]
        price_type = price_type_list[row_num]

        rates_to_push_list, append_rate_to_push_output = append_rates_to_push(brand, segment_id, partner_id, advertiser_id, price, price_type, rates_to_push_list)
        
        if "api_error" in append_rate_to_push_output:
            output_dict[row_num] = append_rate_to_push_output["api_error"]
        row_num += 1

    add_rates_output = add_rate(auth_code, rates_to_push_list)
    if "api_error" in add_rates_output:
        add_rates_output = add_rates_output["api_error"]

    loop_counter = 0
    while loop_counter < number_of_segments:
        if loop_counter in output_dict:
            write_output_list.append(output_dict[loop_counter])
        else:
            write_output_list.append(add_rates_output)
        loop_counter += 1

    write_df = pd.DataFrame({
                                "Segment ID": segment_id_list,
                                "Brand":brand_list,
                                "Partner ID": partner_id_list,
                                "Advertiser ID": advertiser_id_list,
                                "Price": price_list,
                                "Price Type": price_type_list,
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

def get_segments_rates(auth_code, partner_id, advertiser_id, segment_id):
    rates_dict = {}

    # get all Eyeota public taxonomy rates
    result_count = 1
    page_start_index = 0
    while result_count > 0:
        result_count, rates_dict = get_rates(auth_code, EYEOTA_BRAND_ID, page_start_index, partner_id, advertiser_id, rates_dict, segment_id)
        page_start_index += 1

    # get all Bombora public taxonomy rates
    result_count = 1
    page_start_index = 0
    while result_count > 0:
        result_count, rates_dict = get_rates(auth_code, BOMBORA_BRAND_ID, page_start_index, partner_id, advertiser_id, rates_dict, segment_id)
        page_start_index += 1

    return rates_dict

def get_rates(auth_code, brand_id, page_start_index, partner_id, advertiser_id, rates_dict):
    # counter to indicate how many results were returned
    result_count = 0

    json_to_send = {
                        "ProviderId":PROVIDER_ID,
                        "BrandId": brand_id,
                        "RateLevel": PUBLIC_TAXO_RATE_LEVEL,
                        "PageStartIndex": page_start_index,
                        "PageSize":RATES_PAGE_SIZE
                    }
    if not segment_id is None and not partner_id is None:
        json_to_send = {
                            "ProviderId":PROVIDER_ID,
                            "BrandId": brand_id,
                            "RateLevel": PARTNER_TAXO_RATE_LEVEL,
                            "PageStartIndex": page_start_index,
                            "PageSize":RATES_PAGE_SIZE,
                            "PartnerId":partner_id,
                            "ProviderElementIds":[segment_id]
                        }
    elif not segment_id is None and not advertiser_id is None:
        json_to_send = {
                            "ProviderId":PROVIDER_ID,
                            "BrandId": brand_id,
                            "RateLevel": ADVERTISER_TAXO_RATE_LEVEL,
                            "PageStartIndex": page_start_index,
                            "PageSize":RATES_PAGE_SIZE,
                            "PartnerId":advertiser_id,
                            "ProviderElementIds":[segment_id]
                        }

    output_raw_data = requests.post(URL_DATARATE_QUERY,
                    headers={
                        'Content-Type':'application/json',
                        'TTD-Auth': auth_code
                    },
                    json=json_to_send)
    print("Get Rates URL: {}".format(output_raw_data.url))

    rates_data = output_raw_data.json()
    rates_result = rates_data["Result"]

    for each_rate_result in rates_result:
        result_count += 1
        segment_id = str(each_rate_result["ProviderElementId"])

        # get Brand
        brand_id = str(each_rate_result["BrandId"])
        brand = None
        if brand_id == EYEOTA_BRAND_ID:
            brand = "eyeota"
        elif brand_id == BOMBORA_BRAND_ID:
            brand = "bombora"

        # CPM and Percent of Media have different attribute names
        # CPM has amount and currency code
        # Percent of Media only has the rate (in double)
        cpm_price = None
        cpm_currency_code = None
        percent_of_media_cost = None

        try:
            percent_of_media_cost = float(each_rate_result["PercentOfMediaCostRate"])
        except:
            pass

        try:
            cpm_rate = each_rate_result["CPMRate"]
            cpm_price = float(cpm_rate["Amount"])
            cpm_currency_code = cpm_rate["CurrencyCode"]
        except:
            pass

        # Check if segment already exists in rates_dict
        try:
            segment_rates = rates_dict[segment_id]
            if not cpm_price is None:
                segment_rates["CPM_Price"] = cpm_price
                segment_rates["CPM_CurrencyCode"] = cpm_currency_code
            if not percent_of_media_cost is None:
                segment_rates["PercentOfMediaCost"] = percent_of_media_cost
        except:
            rates_dict[segment_id] = {
                "Brand": brand,
                "CPM_Price": cpm_price,
                "CPM_CurrencyCode": cpm_currency_code,
                "PercentOfMediaCost": percent_of_media_cost
            }

    return result_count, rates_dict

def get_segment_rate(segment_id, rates_dict, segment_dictionary):
    if segment_id in rates_dict:
        return rates_dict[segment_id]
    else:
        try:
            parent_segment_id = segment_dictionary[segment_id]["parent_element_id"]
            return get_segment_rate(parent_segment_id, rates_dict, segment_dictionary)
        except:
            return "Segment might be a custom segment which requires Partner ID. Please use 'Retrieve Segment Rates' function for this segment."
        
# based on the output from TTD API, format them into json format to write to file
def processJsonOutput(auth_code, json_output, function):
    # try:
    write_provider_id = []
    write_provider_element_id = []
    write_parent_element_id = []
    write_segment_name = []
    write_display_name = []
    write_buyable = []
    write_description = []
    write_audience_size = []

    segment_dictionary = store_segment_in_dict(json_output)
    rates_dict = get_segments_rates(auth_code, None, None, None)

    # Print results
    for row in json_output['Result']:
        provider_id = str(row['ProviderId'])
        provider_element_id = str(row['ProviderElementId'])
        parent_element_id = str(row['ParentElementId'])
        display_name = str(row['DisplayName'])
        buyable = row['Buyable']
        description = str(row['Description'])
        audience_size = str(row['AudienceSize'])

        cpm_price = None
        cpm_currency_code = None
        percent_of_media_rate = None
        brand = None

        segment_rate = get_segment_rate(provider_element_id, rates_dict, segment_dictionary)

        try:
            cpm_price = segment_rate["CPM_Price"]
            cpm_currency_code = segment_rate["CPM_CurrencyCode"]
            percent_of_media_rate = segment_rate["PercentOfMediaCost"]
            brand = segment_rate["Brand"]
        except:
            cpm_price = "Segment is a custom segment which requires Partner ID. Please use Retrieve Segment Rates function for this segment."

        # loop to get full segment name
        full_display_name = get_full_segment_name(parent_element_id, display_name, segment_dictionary)

        try:
            provider_element_id = int(provider_element_id)
        except:
            pass

        try:
            parent_element_id = int(parent_element_id)
        except:
            pass

        write_provider_id.append(provider_id)
        write_provider_element_id.append(provider_element_id)
        write_parent_element_id.append(parent_element_id)
        write_segment_name.append(display_name)
        write_description.append(description)
        write_buyable.append(buyable)
        write_brand.append(brand)
        write_display_name.append(full_display_name)
        write_audience_size.append(audience_size)
        write_cpm_price.append(cpm_price)
        write_cpm_currency_code.append(cpm_currency_code)
        write_percent_of_media_rate.append(percent_of_media_rate)

    write_df = pd.DataFrame({
                                "Data Provider ID":write_provider_id,
                                "Segment ID":write_provider_element_id,
                                "Parent Segment ID":write_parent_element_id,
                                "Segment Name":write_segment_name,
                                "Segment Description":write_description,
                                "Buyable":write_buyable,
                                "Brand":write_brand,
                                "Full Segment Name":write_display_name,
                                "CPM Price":write_cpm_price,
                                "CPM Currency Code":write_cpm_currency_code,
                                "Percent of Media Rate":write_percent_of_media_rate
                            })
    return write_excel.write_and_email(write_df, "DONOTUPLOAD_The_Trade_Desk_" + function, SHEET_NAME)
    # return write_excel.write(write_df, "DONOTUPLOAD_The_Trade_Desk_" + function, SHEET_NAME)
    # except:
    #     return {"message":"ERROR Processing TTD Json File for Query All Segments"}