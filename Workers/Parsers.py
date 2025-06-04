import pandas as pd
import numpy as np
from datetime import datetime
import re
from urllib import parse
from email.mime.text import MIMEText
import os
import json

def Recieved_time(date): ## Formats datetime into a functional, localized datetime object.

    date = pd.to_datetime(datetime.strptime(date, '%d-%b-%Y %H:%M:%S %z')).tz_convert('America/Chicago') ## INTERNALDATE specified format

    return date

class Parsers():
    def __init__(self): ## loads configuration settings
        with open(os.sys.path[0] + '/Files/settings.json', 'r') as f:
            settings = json.loads(f.read())

        self.url = settings['url'] ## needed for whitelisting photo links from dispatch (safe url's)
        self.subject = settings['subject']
        self.username = settings['username']
        self.reciever = settings['reciever'] ## can be a string or a list
        self.report_reciever = settings['report_reciever'] ## can be a string or a list

    def format_log(self, messages):
        df = pd.DataFrame(messages)

        df['text'] = df['text'].apply(lambda x: x.split('\n')[0] if self.subject in x else np.nan) ## Only matters if the specified subject criterea is present, or else NaN
        df['dt'] = df['ts'].astype(float).apply(datetime.fromtimestamp) ## Converts timestamp string into a datetime object.

        return df

    def compile_actions(self, i, _df, users): ## Disects slack message and responses into a row for tabulation
        idx = _df['dt'].iloc[i]
        text = _df['text'].iloc[i]
        ts = _df['ts'].iloc[i]
        try:
            df = pd.DataFrame(_df['reactions'][i])
            df['ID'] = [text] * len(df)
            df['users'] = df['users'].apply(lambda x: x[0]).apply(
                lambda x: users[users['id'] == x]['name'].iloc[0] if x != None else np.nan)
            df.index = [idx] * len(df)
            df['ts'] = [ts] * len(df)

        except:
            df = pd.DataFrame()
            df['ID'] = [text]
            df['ts'] = [ts]
            df.index = [idx]

        return df

    def format_slack_message(self, message, date, counter): ## Cool features such as adding hyperlinks for addresses and photos.

        message = message.get_payload()

        if isinstance(message, list):
            html = message[1].get_payload()

        elif isinstance(message, str):
            html = message

        text = re.findall(f'({self.subject}(.|\n)*)',
                          html)[0][0]

        text = (text.replace('=\r\n', '')
                    .replace('=20', '')
                    .replace('\r\n', '')
                    .replace('&nbsp;', ' ')
                )
        text = re.sub('<.+?>', '\n', text)
        text = text.split('\n')
        text = list(map(lambda x: x.strip(), text))
        text.remove('')
        text = list(filter(lambda a: a != '', text))
        text = '\n'.join(text)
        text = text.split('If a photo(s) has been attached')[0]

        address = (text.split('Location: ')[-1]
                       .split('\n')[0]
                       .strip()
                   )

        link = 'https://www.google.com/maps/search/?api=1&query=' + \
            parse.quote(address)

        maplink = '<%s|%s>' % (link, address)

        text = text.replace(address, maplink)

        _text = html.replace('=\r\n', '').replace('=20', '')
        s = f'<a href=3D"({self.url}.+?)">'

        photolinks = []

        for x in re.findall(s, _text):
            h = '<%s|Photo Link>' % x

            photolinks.append(h)

        photolinks = ' '.join(photolinks)

        recieved = 'Received: ' + date.strftime('%I:%M %p %m-%d-%Y')

        text = (text + recieved + '\n' + photolinks).strip() + f'\n#{counter}'

        return text

    def format_reply_email(self, message, date): ## Takes the raw email from dispatch and constructs a reply message (Doesnt require the same sender, and can be multiple addresses)

        content = message.get_payload()

        if isinstance(content, list):
            text = content[1].get_payload()

        elif isinstance(content, str):
            text = content

        date = date.strftime('%a, %b %d, %Y at %I:%M %p')

        with open(os.sys.path[0] + '/Files/Reply_Template.html', 'r') as f:
            html = f.read()
            html = html.replace('{{date}}', date)
            html = html.replace('{{text}}', text)

        subject = message['Subject']

        message = MIMEText(html, 'html')
        del(message['Content-Transfer-Encoding'])
        del(message['Content-Type'])

        message['From'] = self.username

        if isinstance(self.reciever, list): ## takes a list for reciever
            message['To'] = ', '.join(self.reciever)
        elif isinstance(self.reciever, str):## takes a string for reciever
            message['To'] = self.reciever

        message['Subject'] = 'Re: ' + subject

        message['Content-Type'] = 'text/html; charset="UTF-8"'
        message['Content-Transfer-Encoding'] = 'quoted-printable'

        message = message.as_string()

        return message
        