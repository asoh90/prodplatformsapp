import write_excel
import appnexus
import adform

ALL_REPORT_PLATFORMS_SHEET_NAME = "All Report Platforms"

def get_report(function, file_path):
    if function == "Data Usage Report":
        return get_platform_reports(file_path, "data_usage")
    elif function == "Volumes Report":
        return get_platform_reports(file_path, "volumes")
    else:
        return {"message":"Error. No such function '{}'".format(function)}

def get_platform_reports(file_path, report_type):
    adform_report_type = None
    appnexus_report_type = None

    # AppNexus get data usage report
    appnexus.get_urls("AppNexus")
    appnexus.authenticate()

    appnexus_segment_dict = None
    if report_type == "data_usage":
        adform_report_type = "data_usage"
        appnexus_report_type = "data_usage"
        appnexus_segment_dict = appnexus.retrieve_all_segments()
    elif report_type == "volumes":
        adform_report_type = "audience"
        appnexus_report_type = "segment_loads"

    # Adform get data usage report
    adform_file_names_output = adform.read_file_to_get_report(file_path, ALL_REPORT_PLATFORMS_SHEET_NAME, adform_report_type)

    # AppNexus get data usage report
    appnexus_file_names_output = appnexus.read_file_to_get_report(file_path, appnexus_report_type, ALL_REPORT_PLATFORMS_SHEET_NAME, appnexus_segment_dict)

    # if "message" is in the output, it is an error message
    if "message" in adform_file_names_output:
        return adform_file_names_output
    elif "message" in appnexus_file_names_output:
        return appnexus_file_names_output
    else:
        file_names = adform_file_names_output + appnexus_file_names_output
        return write_excel.return_report(file_names, ALL_REPORT_PLATFORMS_SHEET_NAME, file_path)