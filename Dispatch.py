#! .venv/bin/python3

from datetime import datetime
from time import sleep
import os
from math import ceil

from Workers import Slack_Functions
from Workers import Email_Functions


def App():
 
    Slack = Slack_Functions() ## Per-use Slack functions
    Email = Email_Functions() ## Per-use Email functions

    boot = True
    report_sent = False
    error_count = 0

    while True:
        try:
            if boot:

                Slack.update_users() ## On the first loop, the user list is generated. (occurs after errors or an update)
                days = 10
                boot = False

            else:
                days = 5
            
            Slack.update_messages(days=days) ## Downloads raw slack messages within a given timeframe

            Slack.update_actions() ## Actions are the various states a request can be in, defined in the settings file
            Slack.apply_filters() ## Creates a table for each important state (Complete, In progress, etc...)
            Slack.post_reminder() ## Alerts staff to check their activity if there is a stale incomplete request. (defined in settings config)
            Slack.clear_duplicates() ## Incase there is a duplicate post in slack due to a network hicup, it deletes the newest request, keeping the original with its timestamp

            Email.update_emails(days=(days - 2)) ## The threshold for emails must be lower than the threshold for slack, otherwise it will repost old requests. 

            pending_posts = Email.inbox[~Email.inbox['Subject'].isin(Slack.actions['ID'])]['Slack'] ## Filters for only ID's not found in the slack channel.
            pending_close = Slack.pending_close ## Filters for completed, but not acknowleged tasks.

            for message in pending_posts:
                Slack.post_message(message=message) ## Posts requests that have been dispatched, but not yet displayed in Slack. 

            for i in range(len(pending_close)):

                ts = pending_close['ts'].iloc[i]
                subject = pending_close['ID'].iloc[i]

                Email.close_job(subject) ## Sends a "formated" email reply to the designated recipients
                Slack.close_job(ts)

            now = datetime.now()

            if now.weekday() == 6 and now.hour == 20 and report_sent == False:  # sunday at 8 pm
                Slack.update_users() ## Updates user info on a weekly basis

                date = now.strftime('%d-%b-%Y') ## Todays Date

                filenames = [f'Completion_Report_{date}.csv',
                             f'Impound_Report_{date}.csv'
                             ]

                files = [Slack.generate_report(metric='Complete').to_csv(),
                         Slack.generate_report(metric='Impounded').to_csv()
                         ]

                Email.send_report(date, filenames, files) ## Composes report with CSV files containing important metrics

                report_sent = True ## Avoids double checking that an email needs to be sent. Even if the program restarts, it wont send duplicates.

            elif now.weekday() != 6: ## Resets the report variable next day
                report_sent = False

            # dynamic rate limiting of 50 requests per min 60/50 = 1.2 (Slack gets mad if we ping them too frequent)
            t = ceil(len(Slack.actions) / 100) * 1.2

            if t != 0:
                sleep(t)
            else:
                sleep(1.2)

            if error_count != 0: ## Congrats! an error free loop
                error_count = 0

                with open(os.sys.path[0] + '/Files/ERRORS.txt', 'a') as f: ## Logs any errors to disk, to help identify an issue later
                    error = datetime.now().strftime('%m/%d %I:%M %p') + ' ' + 'SYSTEM OK'
                    f.write(error + '\n')

            Slack.update_master()

        except KeyboardInterrupt:
            break

        except Exception as e: ## Error handling...
            sleep(1.2)

            Email.refresh_login() ## Refreshes login incase the session expired

            error_count += 1 ## adds to error count.
            error = datetime.now().strftime('%m/%d %I:%M %p') + ' ' + str(e)

            print(error)

            with open(os.sys.path[0] + '/Files/ERRORS.txt', 'a') as f:
                f.write(error + '\n')

            if error_count > 3: ## if there are more than 3 consecutive errors, exit program and try again in 60 seconds.
                break


if __name__ == '__main__':
    App()
