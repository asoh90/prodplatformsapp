from platforms import *
import asyncio

loop = asyncio.get_event_loop()

def callAPI(platform, function, file_path):
    output = {"message":"Platform not found"}

    if platform == "The Trade Desk":
        output = ttd.callAPI(function, file_path)
    elif platform == "Adform":
        output = adform.callAPI(function, file_path)
    # AppNexus has staging and prod environment
    elif platform == "AppNexus" or platform == "AppNexus Staging":
        output = appnexus.callAPI(platform, function, file_path, loop)
    elif platform == "MediaMath":
        output = mediamath.callAPI(platform, function, file_path)
    elif platform == "Adobe AAM":
        output = adobeaam.callAPI(platform, function, file_path)
    elif platform == "Yahoo" or platform == "Yahoo Staging":
        output = yahoo.callAPI(platform, function, file_path)
    elif platform == "All Report Platforms":
        output = all_report_platforms.get_report(function, file_path)
        
    return output