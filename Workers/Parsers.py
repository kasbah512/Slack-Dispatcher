import pandas as pd
import numpy as np
from datetime import datetime
import re
from urllib import parse
from email.mime.text import MIMEText
import os
import json

class Parsers():
    def __init__(self):
        with open(os.sys.path[0] + '/Files/settings.json', 'r') as f:
            settings = json.loads(f.read())

        self.url = settings['url']
        self.subject = settings['subject']
        self.username = settings['username']
        self.reciever = settings['reciever']
        self.report_reciever = settings['report_reciever']

        if isinstance(self.reciever, list):
            self.reciever = ', '.join(self.reciever)
        if isinstance(self.report_reciever, list):
            self.report_reciever = ', '.join(self.report_reciever)

    def format_log(self, messages):
        df = pd.DataFrame(messages)

        df['text'] = df['text'].apply(lambda x: x.split('\n')[0] if self.subject in x else np.nan)
        df['dt'] = df['ts'].astype(float).apply(datetime.fromtimestamp)

        return df

    def compile_actions(self, i, _df, users):
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

    def format_slack_message(self, message): 

        text = re.findall(f'({self.subject}(.|\n)*)',
                          message.get_payload())[0][0]

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

        _text = message.get_payload().replace('=\r\n', '').replace('=20', '')
        s = f'<a href=3D"({self.url}.+?)">'

        photolinks = []

        for x in re.findall(s, _text):
            h = '<%s|Photo Link>' % x

            photolinks.append(h)

        photolinks = ' '.join(photolinks)

        text = (text + photolinks).strip()

        return text

    def format_reply_email(self, message):
        text = message.get_payload().replace('=\r\n', '')
        date = datetime.strptime(message['Date'], '%d %b %Y %H:%M:%S %z').strftime('%a, %b %d, %Y at %I:%M %p')

        with open(os.sys.path[0] + '/Files/Reply_Template.html', 'r') as f:
            html = f.read()
            html = html.replace('{{date}}', date)
            html = html.replace('{{text}}', text)

        subject = message['Subject']

        message = MIMEText(html, 'html')
        del(message['Content-Transfer-Encoding'])
        del(message['Content-Type'])

        message['From'] = self.username

        if isinstance(self.reciever, list):
            message['To'] = ', '.join(self.reciever)
        elif isinstance(self.reciever, str):
            message['To'] = self.reciever

        message['Subject'] = 'Re: ' + subject

        message['Content-Type'] = 'text/html; charset="UTF-8"'
        message['Content-Transfer-Encoding'] = 'quoted-printable'

        message = message.as_string()

        return message
        