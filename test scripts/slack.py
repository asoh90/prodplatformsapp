import requests

URL = "https://hooks.slack.com/services/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"
MESSAGE = "Hi testing 123"

def send_message():
    send_request = requests.post(URL,
                        json={"text":MESSAGE})

    print("Status: {}".format(send_request.status_code))

send_message()