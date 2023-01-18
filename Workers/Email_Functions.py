import pandas as pd
from datetime import datetime, timedelta
import imaplib
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
import json
from wrapt_timeout_decorator import timeout
from Workers import Parsers

class Email_Functions():

    def __init__(self):
        with open(os.sys.path[0] + '/Files/settings.json', 'r') as f:
            settings = json.loads(f.read())

        self.username = settings['username']
        self.password = settings['password']
        
        self.sender = settings['sender']
        self.reciever = settings['reciever']
        self.report_reciever = settings['report_reciever']

        self.subject = settings['subject']

        self.imap = imaplib.IMAP4_SSL(host='imap.gmail.com', port='993')
        self.imap.login(self.username, self.password)
        self.imap.select('"[Gmail]/All Mail"')

        self.Parsers = Parsers()

        self.inbox = pd.DataFrame(
            columns=['Subject', 'Raw', 'Slack', 'Reply'], dtype=object)

    timeout(10)
    def refresh_login(self):
        self.imap = imaplib.IMAP4_SSL(host='imap.gmail.com', port='993')
        self.imap.login(self.username, self.password)
        self.imap.select('"[Gmail]/All Mail"')

    timeout(10)
    def update_emails(self, days = 7):
        response = self.imap.noop()
        assert response[0] == 'OK'

        cut = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")
        query = f'From "{self.sender}" Subject "{self.subject}" SINCE "{cut}"'

        response = self.imap.search(None, query)

        assert response[0] == 'OK'

        inbox = response[1][0].decode().split(' ')

        assert len(inbox) > 1

        self.inbox = self.inbox[self.inbox.index.isin(inbox)]

        for inbox_id in inbox:

            if inbox_id not in self.inbox.index:
                response = self.imap.fetch(inbox_id, 'rfc822')
                assert response[0] == 'OK'

                _message = response[1][0][1]
                message = email.message_from_bytes(_message)

                self.inbox.loc[inbox_id] = [None] * len(self.inbox.columns)

                self.inbox['Subject'].loc[inbox_id] = message['Subject']
                self.inbox['Raw'].loc[inbox_id] = message
                self.inbox['Slack'].loc[inbox_id] = self.Parsers.format_slack_message(message)
                self.inbox['Reply'].loc[inbox_id] = self.Parsers.format_reply_email(message)

        return(self.inbox)

    @timeout(10)
    def close_job(self, subject, force = False):

        query = f'From "{self.username}" Subject "{subject}"'
        sent_id = self.imap.search(None, query)[1][0].decode().split(' ')[-1]

        if sent_id == '' or force:

            smtp = smtplib.SMTP('smtp.gmail.com', '587')
            smtp.starttls()
            smtp.login(self.username, self.password)

            message = self.inbox[self.inbox['Subject'] == subject]['Reply'].iloc[0]
            smtp.sendmail(self.username, self.reciever, message)

            self.imap.noop()

            sent_id = self.imap.search(None, query)[1][0].decode().split(' ')[-1]

            assert sent_id != ''

    @timeout(10)
    def send_report(self, date, filename, file, force = False):

        inbox_id = self.imap.search(None, f'Subject "Ops Report" On "{date}"')[1][0].decode()

        if inbox_id == '' or force:
            sender = self.username
            password = self.password

            message = MIMEMultipart()
            message['From'] = sender

            if isinstance(self.report_reciever, list):
                message['To'] = ', '.join(self.report_reciever)
            else:
                message['To'] = self.report_reciever

            message['Subject'] = 'Ops Report'

            payload = MIMEBase('application', 'csv', Name=filename)
            payload.set_payload(file)
            encoders.encode_base64(payload)  # encode the attachment
            payload.add_header('Content-Decomposition',
                            'attachment', filename=filename)
            message.attach(payload)

            session = smtplib.SMTP('smtp.gmail.com', '587')  # use gmail with port
            session.starttls()  # enable security

            session.login(sender, password)  # login with mail_id and password

            text = message.as_string()

            session.sendmail(sender, self.report_reciever, text)
        
            self.imap.noop()

            inbox_id = self.imap.search(None, f'Subject "Ops Report" On "{date}"')[1][0].decode()

            assert inbox_id != ''

    def print_messages(self):
        self.inbox['Slack'].apply(lambda x: print(x + '\n' + '#' * 150))
        