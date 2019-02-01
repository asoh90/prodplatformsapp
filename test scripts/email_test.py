import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

gmail_user = 'data@eyeota.com'
gmail_password = '3yEoT@D8a-2019'

recipient = 'asoh@eyeota.com'

msg = MIMEMultipart()
msg['From'] = gmail_user
msg['To'] = recipient
msg['Subject'] = "This is a test email"

body = """\
<html>
  <head></head>
  <body>
    <p>Hi!<br>
       How are you?<br>
       Here is the <a href="http://www.python.org">link</a> you wanted.
    </p>
  </body>
</html>
"""
msg.attach(MIMEText(body,'html'))

# try:
server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
server.login(gmail_user, gmail_password)
server.sendmail(gmail_user, recipient, msg.as_string())

server.close()

print("Email sent")
# except:
#     print("Failed")