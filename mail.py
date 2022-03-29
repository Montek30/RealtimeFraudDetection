from email.header    import Header
from email.mime.text import MIMEText
from getpass         import getpass
from smtplib         import SMTP_SSL


login, password = 'monteks764@gmail.com', "heyhey123@H"
recipients = ["monteksingh30@gmail.com"]

# create message
msg = MIMEText('Alert! Fraudulent Transaction has been detected from your carf', 'plain', 'utf-8')
msg['Subject'] = "test"
msg['From'] = login
msg['To'] = ", ".join(recipients)

# send it via gmail
s = SMTP_SSL('smtp.gmail.com', 465, timeout=10)
# s.set_debuglevel(1)
try:
    t = s.login(login, password)
    print(t)

    r = s.sendmail(msg['From'], recipients, msg.as_string())
    print(r)
finally:
    s.quit()