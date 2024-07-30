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
from Workers import Parsers, Recieved_time

class Email_Functions():

    @timeout(10)    ## raises error if not complete after n seconds
    def __init__(self):
        with open(os.sys.path[0] + '/Files/settings.json', 'r') as f:
            settings = json.loads(f.read())

        self.username = settings['username']    ## gmail email address
        self.password = settings['password']    ## gmail app password
        
        self.sender = settings['sender']        ## email address for dispatch
        self.reciever = settings['reciever']    ## email address/'s for closing request reponse
        self.report_reciever = settings['report_reciever']  ## manager to recieve email 

        self.subject = settings['subject']      ## desired email subject pattern for imap search

        self.imap = imaplib.IMAP4_SSL(host='imap.gmail.com', port='993') ## launches imap instance
        self.imap.login(self.username, self.password)   ## logs into imap instance
        self.imap.select('"[Gmail]/All Mail"')  ## selects both inbox and sent mail for searches

        self.Parsers = Parsers()    ## parsers file catered to specific needs

        self.inbox = pd.DataFrame(
            columns=['Subject', 'Raw', 'Slack', 'Reply', 'Date', 'Counter'], dtype=object)

    @timeout(10)    ## raises error if not complete after n seconds
    def refresh_login(self):    ## refreshes login if expired
        self.imap = imaplib.IMAP4_SSL(host='imap.gmail.com', port='993')
        self.imap.login(self.username, self.password)
        self.imap.select('"[Gmail]/All Mail"')

    @timeout(300)    ## raises error if not complete after n seconds
    def update_emails(self, days = 7):
        response = self.imap.noop() ## refreshes mail data
        assert response[0] == 'OK'  ## ensures action was success, raises error if not

        cut = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y") ## cutoff date for search query

        if isinstance(self.sender, str):
            query = f'From "{self.sender}" Subject "{self.subject}" SINCE "{cut}"' ## Imap protocol search query

        elif isinstance(self.sender, list):
            query = 'OR'

            for sender in self.sender:
                query += f' (From "{sender}" Subject "{self.subject}" SINCE "{cut}")'
        
        response = self.imap.search(None, query)    ## response from imap server

        assert response[0] == 'OK'  ## ensures action was success, raises error if not

        inbox = response[1][0].decode().split(' ')  ## Imap protocol inbox id list [1, 2, 3, ....]

        assert len(inbox) > 1   ## ensures inbox length is > 1. *** double check edge case of only 1 item in inbox for search query 

        self.inbox = self.inbox[self.inbox.index.isin(inbox)] ## filters out emails before cutoff

        counter = 1 ## initializes counter for slack posts
        initial = (datetime.now() - timedelta(days=days)).date() ## this is so the counter can reset if the date of the email is not the same as the previous message

        for inbox_id in inbox: ## Imap protocol inbox id 

            if inbox_id not in self.inbox.index:
                response = self.imap.fetch(inbox_id, 'rfc822')
                assert response[0] == 'OK'

                _message = response[1][0][1] ## raw email data

                message = email.message_from_bytes(_message) ## formated email data
                date = Recieved_time(message['Date']).date() ## date the email was recieved

                self.inbox.loc[inbox_id] = [None] * len(self.inbox.columns) ## initializes empty row for inbox id n

                self.inbox['Subject'].loc[inbox_id] = message['Subject']    ## email subject
                self.inbox['Raw'].loc[inbox_id] = message                   ## "raw" email data
                self.inbox['Reply'].loc[inbox_id] = self.Parsers.format_reply_email(message)    ## closing response reply message (creates thread)
                self.inbox['Date'].loc[inbox_id] = date                     ## date the email was recieved

            else:
                message = self.inbox['Raw'].loc[inbox_id]
                
            if self.inbox['Date'].loc[inbox_id] == initial:
                counter += 1
            else:
                counter = 1
                initial = self.inbox['Date'].loc[inbox_id]

            self.inbox['Slack'].loc[inbox_id] = self.Parsers.format_slack_message(message, counter)
            self.inbox['Counter'].loc[inbox_id] = counter

        return(self.inbox)

    @timeout(10)    ## raises error if not complete after n seconds
    def close_job(self, subject, force = False):

        query = f'From "{self.username}" Subject "{subject}"' ## Imap protocol search query
        sent_id = self.imap.search(None, query)[1][0].decode().split(' ')[-1]

        if sent_id == '' or force:  ## ensures that the closing email has not been sent yet, to avoid duplicates

            smtp = smtplib.SMTP('smtp.gmail.com', '587')
            smtp.starttls()
            smtp.login(self.username, self.password)

            message = self.inbox[self.inbox['Subject'] == subject]['Reply'].iloc[0]
            smtp.sendmail(self.username, self.reciever, message)

            self.imap.noop()

            sent_id = self.imap.search(None, query)[1][0].decode().split(' ')[-1]

            assert sent_id != ''

    @timeout(10)    ## raises error if not complete after n seconds
    def send_report(self, date, filenames, files, force = False):

        inbox_id = self.imap.search(None, f'Subject "Ops Report" On "{date}"')[1][0].decode()

        if inbox_id == '' or force: ## ensures that the report has not been sent yet, to avoid duplicates
            sender = self.username
            password = self.password

            message = MIMEMultipart()
            message['From'] = sender

            if isinstance(self.report_reciever, list):
                message['To'] = ', '.join(self.report_reciever)
            else:
                message['To'] = self.report_reciever

            message['Subject'] = 'Ops Report'

            assert isinstance(files, list)
            assert isinstance(filenames, list)
            assert len(files) == len(filenames)

            for i in range(len(files)):

                payload = MIMEBase('application', 'csv', Name=filenames[i])
                payload.set_payload(files[i])
                encoders.encode_base64(payload)  # encode the attachment
                payload.add_header('Content-Decomposition',
                            'attachment', filename=filenames[i])
                message.attach(payload)

            session = smtplib.SMTP('smtp.gmail.com', '587')  # use gmail with port
            session.starttls()  # enable security

            session.login(sender, password)  # login with mail_id and password

            text = message.as_string()

            session.sendmail(sender, self.report_reciever, text)
        
            self.imap.noop()

            inbox_id = self.imap.search(None, f'Subject "Ops Report" On "{date}"')[1][0].decode()

            assert inbox_id != ''

    def print_messages(self): ## for printing off all the messages that should be posted for sanity check
        self.inbox['Slack'].apply(lambda x: print(x + '\n' + '#' * 150))
        