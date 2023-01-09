#!/usr/bin/env python 

from datetime import datetime
from time import sleep
import os

from Workers import Slack_Functions
from Workers import Email_Functions

def App():
    Slack = Slack_Functions()
    Email = Email_Functions()

    Slack.update_users()

    boot = True
    active = True
    report_sent = False
    error_count = 0

    while True:
        try:
            if boot:
                days = 10
                boot = False

            elif active:
                days = 2
            
            Slack.update_messages(days = days)
            
            Slack.update_actions()
            Slack.apply_filters()
            Slack.post_reminder()
            
            Email.update_emails()

            pending_posts = Email.inbox[~Email.inbox['Subject'].isin(Slack.actions['ID'])]['Slack']

            if len(pending_posts) > 0:
                active = True

            elif len(Slack.pending_service) == 0:
                active = False

            for message in pending_posts:
                Slack.post_message(message = message)

            if len(Slack.pending_close) > 0:
                
                pending_close = Slack.pending_close

                for i in range(len(pending_close)):

                    ts = pending_close['ts'].iloc[i]
                    subject = pending_close['ID'].iloc[i]

                    Email.close_job(subject)
                    Slack.close_job(ts)

            now = datetime.now()

            if now.weekday() == 0 and now.hour == 9 and report_sent == False: ### monday at 9 am
                Slack.update_users()

                date = now.strftime('%d-%b-%Y')
                filename = f'Ops_Report_{date}.csv'
                file = Slack.generate_report().to_csv()

                Email.send_report(date, filename, file)

                report_sent = True

            elif now.weekday() != 0:
                report_sent = False

            sleep(2)

        except KeyboardInterrupt:
            break

        except Exception as e:

            Email.refresh_login()

            error_count += 1
            error = datetime.now().strftime('%m/%d %I:%M %p') + ' ' + str(e)

            print(error)
            
            with open(os.sys.path[0] + '/Files/ERRORS.txt', 'a') as f:
                f.write(error + '\n')

            if error_count > 3:
                break

if __name__ == '__main__':
    App()
    