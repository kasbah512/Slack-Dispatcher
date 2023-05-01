import slack_sdk as slack
import pandas as pd
from datetime import datetime, timedelta
from functools import partial
import os
import json
from wrapt_timeout_decorator import *
from Workers import Parsers

class Slack_Functions():

    timeout(10)
    def __init__(self):
        with open(os.sys.path[0] + '/Files/settings.json', 'r') as f:
            settings = json.loads(f.read())

        self.token = settings['token']
        self.channel = settings['channel']

        self.accepted_symbol = settings["accepted_symbol"]
        self.completed_symbol = settings["completed_symbol"]
        self.closed_symbol = settings["closed_symbol"]
        self.impound_symbol = settings["impound_symbol"]

        self.acceptance_threshold = settings['acceptance threshold']
        self.service_threshold = settings['service threshold']
        self.re_alert = settings['re-alert']

        self.reminder_start = settings['reminder_start']
        self.reminder_stop = settings['reminder_stop']
        self.reminder_message = settings['reminder_message']
        self.reminder_ts = None

        self.Parsers = Parsers()

        self.client = slack.WebClient(token=self.token)

    timeout(10)
    def update_users(self):

        users = timeout(dec_timeout=10)(
            self.client.users_list())['members']

        df = pd.DataFrame(users)
        df['name'] = df['profile'].apply(lambda x: x['real_name'])
        df = df[['name', 'id']]

        self.users = df

        return(self.users)

    timeout(10)
    def update_messages(self, days):
        
        oldest = oldest=(datetime.now() - timedelta(days=days)).timestamp()

        self.message_log = timeout(dec_timeout=10)(
            self.client.conversations_history)(channel=self.channel, oldest = oldest, limit=1000).data

        assert self.message_log['ok'] == True

    def update_actions(self):

        messages = self.message_log['messages']
        df = self.Parsers.format_log(messages)

        _actions = pd.concat(map(partial(self.Parsers.compile_actions, users=self.users, _df=df), range(len(df))))

        actions = pd.DataFrame(index=_actions.index)

        actions['ts'] = _actions['ts']
        actions['ID'] = _actions['ID']
        actions['Accepted'] = _actions[_actions['name']
                                       == self.accepted_symbol]['users']
        actions['Complete'] = _actions[_actions['name']
                                       == self.completed_symbol]['users']
        actions['Closed'] = _actions[_actions['name']
                                       == self.closed_symbol]['users']
        actions['Impounded'] = _actions[_actions['name']
                                       == self.impound_symbol]['users']
        
        actions['Impounded'] = actions['Impounded'].mask(actions['Impounded'].isna() == False, actions['Complete']) ## properly gives credit to whoever completed the task

        try:
            self.actions = pd.concat([actions, self.actions])
        except AttributeError:
            self.actions = actions
        finally:
            self.actions = self.actions[~self.actions.index.duplicated(
                keep='first')].sort_index(ascending=False)

        self.actions[self.actions.index > datetime.now() - timedelta(days = 10)]
        self.actions = self.actions[~self.actions.isna().all(axis=1)]

        return(self.actions)

    def apply_filters(self):
        self.pending_acceptance = self.actions[(self.actions['ID'].isna() == False)  # only tasks from dispatch
                                               & (self.actions['Accepted'].isna())
                                               ]

        self.pending_service = self.actions[(self.actions['ID'].isna() == False)  # only tasks from dispatch
                                            & (self.actions['Complete'].isna())
                                            & (self.actions['Closed'].isna())
                                            ]

        self.pending_close = self.actions[(self.actions['ID'].isna() == False)  # only tasks from dispatch
                                          & (self.actions['Complete'].isna() == False)
                                          & (self.actions['Closed'].isna())
                                          ]

        self.warn_acceptance = self.pending_acceptance[self.pending_acceptance.index < datetime.now() - timedelta(minutes=self.acceptance_threshold)]
        self.warn_service = self.pending_service[self.pending_service.index < datetime.now() - timedelta(minutes=self.service_threshold)]

    def generate_report(self, metric = 'Complete'):

        dates = pd.date_range(datetime.now() - timedelta(days=6),
                              datetime.now()).date

        report = []
        for date in dates:
            report.append(self.actions[self.actions.index.date ==
                                             date][metric].value_counts()
            )


        report = pd.concat(report, axis=1)
        self.report = report

        self.report.columns = dates

        self.report.loc['Total'] = self.report.sum(axis=0)
        self.report['Sum'] = self.report.sum(axis=1)
        self.report = self.report.fillna(0).astype(int)
        self.report = self.report.sort_values(by='Sum', ascending=False)

        return(self.report)

    timeout(10)
    def post_message(self, message):

        response = self.client.chat_postMessage(channel=self.channel,
                                                text=message,
                                                unfurl_links=False,
                                                unfurl_media=False
        )
        assert response['ok'] == True

    timeout(10)
    def close_job(self, ts):

        response = self.client.reactions_add(channel=self.channel,
                                             timestamp=ts,
                                             name=self.closed_symbol
        )
        assert response['ok'] == True

    timeout(10)
    def post_reminder(self, reminder_ts = None): ## fix to handle ts after crash
        if reminder_ts != None:
            self.reminder_ts = reminder_ts
            
        now = datetime.now()

        start = datetime.strptime(f'{now.date()} {self.reminder_start}', '%Y-%m-%d %I:%M %p')
        stop = datetime.strptime(f'{now.date()} {self.reminder_stop}', '%Y-%m-%d %I:%M %p')

        df = pd.DataFrame(self.message_log['messages'])
        ts = df[df['text'] == self.reminder_message]['ts'].astype(float)

        if (start <= now and now < stop) and (len(self.warn_acceptance) + len(self.warn_service) > 0):

            if len(ts) == 0:
                response = self.client.chat_postMessage(channel=self.channel,
                                                        text=self.reminder_message

                )
                assert response['ok'] == True

                self.reminder_ts = float(response['ts'])
            
            elif now - timedelta(minutes=self.re_alert) > datetime.fromtimestamp(ts.iloc[0]):

                response = self.client.chat_delete(channel=self.channel,
                                                   ts=ts.iloc[0]
                )
                assert response['ok'] == True

                response = self.client.chat_postMessage(channel=self.channel,
                                                        text=self.reminder_message
                )
                assert response['ok'] == True

        elif len(ts) > 0:

            for _ts in ts:
                response = self.client.chat_delete(channel=self.channel,
                                                   ts= _ts
                )
                assert response['ok'] == True
                
