#! .venv/bin/python3

from datetime import datetime
from time import sleep
import os
from math import ceil

from Workers import Slack_Functions
from Workers import Email_Functions


def App():

    Slack = Slack_Functions()
    Email = Email_Functions()

    boot = True
    report_sent = False
    error_count = 0

    while True:
        try:
            if boot:
                Slack.update_users()
                days = 10
                boot = False

            else:
                days = 5

            Slack.update_messages(days=days)

            Slack.update_actions()
            Slack.apply_filters()
            Slack.post_reminder()
            Slack.clear_duplicates()

            Email.update_emails(days=(days - 2))

            pending_posts = Email.inbox[~Email.inbox['Subject'].isin(
                Slack.actions['ID'])]['Slack']
            pending_close = Slack.pending_close

            for message in pending_posts:
                Slack.post_message(message=message)

            for i in range(len(pending_close)):

                ts = pending_close['ts'].iloc[i]
                subject = pending_close['ID'].iloc[i]

                Email.close_job(subject)
                Slack.close_job(ts)

            now = datetime.now()

            if now.weekday() == 6 and now.hour == 20 and report_sent == False:  # sunday at 8 pm
                Slack.update_users()
                Slack.update_master()

                date = now.strftime('%d-%b-%Y')

                filenames = [f'Completion_Report_{date}.csv',
                             f'Impound_Report_{date}.csv'
                             ]

                files = [Slack.generate_report(metric='Complete').to_csv(),
                         Slack.generate_report(metric='Impounded').to_csv()
                         ]

                Email.send_report(date, filenames, files)

                report_sent = True

            elif now.weekday() != 6:
                report_sent = False

            # dynamic rate limiting of 50 requests per min 60/50 = 1.2
            t = ceil(len(Slack.actions) / 100) * 1.2

            if t != 0:
                sleep(t)
            else:
                sleep(1.2)

            if error_count != 0:
                error_count = 0

                with open(os.sys.path[0] + '/Files/ERRORS.txt', 'a') as f:
                    error = datetime.now().strftime('%m/%d %I:%M %p') + ' ' + 'SYSTEM OK'
                    f.write(error + '\n')

        except KeyboardInterrupt:
            break

        except Exception as e:
            sleep(1.2)

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
