import slack_sdk as slack
import pandas as pd
from datetime import datetime, timedelta
from functools import partial
import os
import json
from wrapt_timeout_decorator import *
from Workers import Parsers

class Slack_Functions():

    @timeout(10)
    def __init__(self): ## Loads configuration settings
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

    @timeout(10)
    def update_users(self): ## Turns json format into a usable DataFrame. for translating ID Codes to User names 

        users = timeout(dec_timeout=10)(
            self.client.users_list())['members']

        df = pd.DataFrame(users)
        df['name'] = df['profile'].apply(lambda x: x['real_name'])
        df = df[['name', 'id']]

        self.users = df

        return(self.users)

    @timeout(10)
    def update_messages(self, days): ## Fetches current slack messages within a specified timeframe
        
        oldest = oldest=(datetime.now() - timedelta(days=days)).timestamp()

        self.message_log = timeout(dec_timeout=10)(
            self.client.conversations_history)(channel=self.channel, oldest = oldest, limit=1000).data ## limits to 1k messages. (if we have more than 1k active, we will have bigger problems)

        assert self.message_log['ok'] == True

    def update_actions(self, cutoff = 10):

        messages = self.message_log['messages']
        df = self.Parsers.format_log(messages)

        _actions = pd.concat(map(partial(self.Parsers.compile_actions, users=self.users, _df=df), range(len(df)))) ## one single row (unformatted)

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
            self.actions = self.actions[~self.actions.index.duplicated(keep='first')].sort_index(ascending=False)

        self.actions = self.actions[self.actions.index > datetime.now() - timedelta(days = cutoff)]
        self.actions = self.actions[~self.actions.isna().all(axis=1)]

        return(self.actions)

    def apply_filters(self):
        self.pending_acceptance = self.actions[(self.actions['ID'].isna() == False)  ## only tasks from dispatch
                                               & (self.actions['Accepted'].isna())   ## Has not been accepted
                                               ]

        self.pending_service = self.actions[(self.actions['ID'].isna() == False)    ## only tasks from dispatch
                                            & (self.actions['Complete'].isna())     ## is not complete
                                            & (self.actions['Closed'].isna())       ## is not closed (could be manually closed by others)
                                            ]

        self.pending_close = self.actions[(self.actions['ID'].isna() == False)          ## only tasks from dispatch
                                          & (self.actions['Complete'].isna() == False)  ## is complete
                                          & (self.actions['Closed'].isna())             ## is not closed (could be manually closed by others)
                                          ]

        self.warn_acceptance = self.pending_acceptance[(self.pending_acceptance.index < datetime.now() - timedelta(minutes=self.acceptance_threshold)) & ## has not been marked as accepted in a timely manner, and is within the agreed upon hours
                                                       (self.pending_acceptance['Closed'].isna())]
        
        self.warn_service = self.pending_service[self.pending_service.index < datetime.now() - timedelta(minutes=self.service_threshold)] ## hasnt been completed in a reasonable time, and is within the agreed upon hours

    def generate_report(self, metric = 'Complete'): ## creates a table for the past week colums are days, and rows are employees.

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

    @timeout(10)
    def post_message(self, message): ## posts to slack

        response = self.client.chat_postMessage(channel=self.channel,
                                                text=message,
                                                unfurl_links=False, ## wont render webpages in slack
                                                unfurl_media=False, ## wont render images in slack
                                                parse="none",       ## Prevents typos from being converted to hyperlinks
        )
        assert response['ok'] == True ## ensures the message was received

    @timeout(10)
    def clear_duplicates(self): ## keeps oldest message with same ID and deletes newer duplicates (IF needed)
        for ts in self.actions[self.actions['ID'].duplicated(keep='last') & (self.actions['ID'].isna() == False)]['ts']: ## deletes duplicates
            self.client.chat_delete(channel=self.channel, ts=ts)
            self.actions = self.actions[self.actions['ts'] != ts] ## removes message from log

    @timeout(10)
    def close_job(self, ts): ## reacts with emoji ti the required post

        response = self.client.reactions_add(channel=self.channel,
                                             timestamp=ts,
                                             name=self.closed_symbol
        )
        assert response['ok'] == True ## ensures the message was recieved

    def update_master(self): ## Appends master record with current data

        path = os.sys.path[0] + '/Files/Master_Record.csv'
        
        try:
            df = pd.read_csv(path, index_col=0)
            df.index = pd.to_datetime(df.index)

            df = pd.concat([self.actions, df])
            
            df = df[~df.index.duplicated(keep='first')]
            df.to_csv(path)

        except FileNotFoundError:

            self.actions.to_csv(path)

    @timeout(10)
    def post_reminder(self, reminder_ts = None): ## fix to handle ts after crash ## (I dont remember what the actual issue was here)
        if reminder_ts != None:
            self.reminder_ts = reminder_ts
            
        now = datetime.now()

        start = datetime.strptime(f'{now.date()} {self.reminder_start}', '%Y-%m-%d %I:%M %p')
        stop = datetime.strptime(f'{now.date()} {self.reminder_stop}', '%Y-%m-%d %I:%M %p')

        df = pd.DataFrame(self.message_log['messages'])
        ts = df[df['text'].apply(lambda x: 'Active Requests' in x)]['ts'].astype(float)

        if (start <= now and now < stop) and (len(self.warn_acceptance) + len(self.warn_service) > 0): ## in the agreed upon time, and hasnt been posted within X amount of time

            df = pd.concat([self.warn_acceptance, self.warn_service]).sort_index()
            request_ts = str(df['ts'].iloc[0])
            request_ts = request_ts.replace('.', '')
            
            message = self.reminder_message
            
            url = f'https://limeops-austin.slack.com/archives/{self.channel}/p{request_ts}' ## url for the oldest active post
            link = f'<{url}|Active Requests>'

            message = message.replace('Active Requests', link)

            if len(ts) == 0:
                response = self.client.chat_postMessage(channel=self.channel,
                                                        text=message,
                                                        unfurl_links=False,
                                                        unfurl_media=False

                )
                assert response['ok'] == True

                self.reminder_ts = float(response['ts'])
            
            elif now - timedelta(minutes=self.re_alert) > datetime.fromtimestamp(ts.iloc[0]):

                response = self.client.chat_delete(channel=self.channel,
                                                   ts=ts.iloc[0]
                )
                assert response['ok'] == True

                response = self.client.chat_postMessage(channel=self.channel,
                                                        text=message,
                                                        unfurl_links=False,
                                                        unfurl_media=False
                )
                assert response['ok'] == True

        elif len(ts) > 0:

            for _ts in ts: ## on the off chance it double posts, it will delete all reminders after they are no longer required.
                response = self.client.chat_delete(channel=self.channel,
                                                   ts= _ts
                )
                assert response['ok'] == True
                
