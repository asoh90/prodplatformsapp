SECRET_KEY = 'a1ac33ec538de1e200d5f537e717ae6b'
UPLOAD_FOLDER = "upload"
RETURN_FOLDER = "to_return"

# S3 credentials
S3_URL = 'http://localhost:8000'
AWS_KEY = '123'
AWS_SECRET = 'abc'

# Postgresql credentials
POSTGRES_DB = 'eyeota'
POSTGRES_USER = 'eyeota'
POSTGRES_PASSWORD = '3y3otaSG!!'
POSTGRES_HOST = 'localhost'

CURRENT_URL = "http://127.0.0.1:5000"
login_credentials = {}

# Pool for multithreading
thread_pool_dict = {}

# Logger for console messages
logger = None

dataops_emails = {
                    "dgoh@eyeota.com":1,
                    "clim@eyeota.com":2,
                    "ssatish@eyeota.com":2,
                    "sychong@eyeota.com":2,
                    "asoh@eyeota.com":1,
                    "eso@eyeota.com":2,
                    "mor@eyeota.com":2
                }

def read_credentials(input):
    input_list = input.split("||")

    for input_row in input_list:
        # print("input row: " + input_row)
        input_row_list = input_row.split(":")

        platform = input_row_list[0]
        key = input_row_list[1]
        value = input_row_list[2]

        if platform in login_credentials:
            login_credentials[platform][key] = value
        else:
            login_credentials[platform] = {key:value}

    # print(login_credentials)

def get_platform_functions(email):
    platform_functions = None
    global dataops_emails

    if email in dataops_emails:
        platform_functions = {
                                "--Select Platform--":[],
                                "Adform":{"level":2,
                                            "functions":{
                                                "Segment":["Add Segments", "Edit Segments", "Query All Segments"],
                                                "Report":["Audience Report","Data Usage Report"],
                                                "Delete Segment":["Delete Segments"]
                                            }
                                        },
                                "Adobe AAM":{"level":2,
                                            "functions":{
                                                "Segment":["Add Segments", "Edit Segments", "Query All Segments"],
                                                "Others":["Get Data Source Uniques", "Get Trait Rule", "Query Subscriber Contacts"]
                                                }
                                            },
                                "Adobe AdCloud":{"level":1,
                                            "functions":["Add Custom Segments", "Edit Custom Segments"]
                                            },
                                "AppNexus Staging": {"level":2,
                                            "functions": {
                                                "Segment":["Add Segments","Edit Segments","Query All Segments","Retrieve Segments"],
                                                "Report":["Data Usage Report", "Segment Loads Report"],
                                                "Troubleshoot":["Add Existing Segments to Specific Buyer Member","Add Segment Billings","Retrieve Buyer Member Segments"]
                                                }
                                            },
                                "AppNexus": {"level":2,
                                            "functions": {
                                                "Segment":["Add Segments","Edit Segments","Query All Segments","Retrieve Segments"],
                                                "Report":["Data Usage Report", "Segment Loads Report"],
                                                "Troubleshoot":["Add Existing Segments to Specific Buyer Member","Add Segment Billings","Retrieve Buyer Member Segments"]
                                                }
                                            },
                                "MediaMath": {"level":1,
                                            "functions":["Refresh Segments","Query All Segments"]
                                            },
                                "The Trade Desk": {"level":2,
                                                    "functions":{
                                                        "Segment":["Add Segments","Edit Segments","Query All Segments", "Retrieve Batch Status"],
                                                        "Segment Rates":["Edit Segment Rates", "Retrieve Partner Rates"]
                                                    }
                                                },
                                "Yahoo Staging":{"level":1,
                                            "functions":["Refresh Segments","Query All Segments"]
                                    },
                                "Yahoo":{"level":1,
                                            "functions":["Refresh Segments","Query All Segments"]
                                    },
                                "All Report Platforms":{"level":1,
                                            "functions":["Data Usage Report","Volumes Report"]
                                }
                            }
    else:
        platform_functions = {
                                "--Select Platform--":[],
                                "Adform":{"level":2,
                                            "functions":{
                                                "Segment":["Query All Segments"],
                                                "Report":["Audience Report","Data Usage Report"],
                                            }
                                        },
                                "Adobe AAM":{"level":2,
                                            "functions":{
                                                "Segment":["Query All Segments"],
                                                "Others":["Get Data Source Uniques", "Get Trait Rule", "Query Subscriber Contacts"]
                                                }
                                            },
                                "AppNexus": {"level":2,
                                            "functions": {
                                                "Segment":["Query All Segments"],
                                                "Report":["Data Usage Report", "Segment Loads Report"],
                                                }
                                            },
                                "MediaMath": {"level":1,
                                            "functions":["Query All Segments"]
                                            },
                                "The Trade Desk": {"level":1,
                                                    "functions":["Query All Segments"],
                                                },
                                "Yahoo":{"level":1,
                                            "functions":["Query All Segments"]
                                    },
                                "All Report Platforms":{"level":1,
                                            "functions":["Data Usage Report","Volumes Report"]
                                }
                            }
    return platform_functions