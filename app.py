import os
import re
import logging
import slack
import ssl as ssl_lib
import certifi
import threading
import datetime
import math
import time
from multiprocessing import Pool

from db import Database
from stuffquiz import StuffQuiz, StuffQuizPoller


QUIZ_CHANNEL    = '#quizscores'
DATABASE_NAME   = 'quiz-scorer.db'

QUIZ_DAYS_OF_WEEK = (0, 1, 2, 3, 4)

PROCESS_POOL = None


def get_channel_name(channel_id, web_client):
    with Database(DATABASE_NAME) as db:
        # get the channel from persistence
        channel_name = db.get_channel_name_by_id(channel_id)
        if channel_name:
            return channel_name
        # get the channel name from slack
        if channel_id.startswith('G'):
            # private channel
            response = web_client.groups_info(channel=channel_id)
            channel_name = response.data['group']['name']
        elif channel_id.startswith('C'):
            # ordinary channel
            response = web_client.channels_info(channel=channel_id)
            channel_name = response.data['channel']['name']
        else:
            print(f'Unknown channel type: {channel_id}')
            return None
        # store the channel name
        db.create_channel(channel_id, channel_name)
        return channel_name


def get_user_name(user_id, web_client):
    with Database(DATABASE_NAME) as db:
        # get the user from persistence
        user_name = db.get_user_name_by_id(user_id)
        if user_name:
            return user_name
        # get the user name from slack
        if user_id.startswith('U'):
            # regular user
            response = web_client.users_info(user=user_id)
            user_name = response.data['user']['profile']['display_name']
        else:
            print(f'Unknown user type: {user_id}')
            return None
        # store the user name
        db.create_user(user_id, user_name)
        return user_name


def add_reaction(name, channel_id, ts, web_client):
    try:
        web_client.reactions_add(
            name=name,
            channel=channel_id,
            timestamp=ts
        )
    except Exception as e:
        print(f'exception while adding reaction: {e}')


def strip_emojis(line):
    return re.sub(r':[0-9A-Za-z._-]+:', '', line)


def parse_text_for_marker(text, markers):
    for marker in markers:
        if marker in text:
            return True
    return False


def parse_text_for_morning(text):
    return parse_text_for_marker(text.lower(), ('am', 'a.m', 'morning'))


def parse_text_for_afternoon(text):
    return parse_text_for_marker(text.lower(), ('pm', 'p.m', 'afternoon'))


def parse_text_for_yesterday(text):
    return parse_text_for_marker(text.lower(), ('yesterday',))


def parse_text_for_scores(text):
    '''
    returns scores in the given text as a list of
    [
        (score, is_am, is_pm, is_yesterday)
        ...
    ]
    '''
    parsed_scores = []
    for line in text.split('\n'):
        try:
            text_scores = re.findall(r'([0-9]{1,2})/15', line)
        except Exception as e:
            print(f'exception parsing text for score: {e} ({text})')
        else:
            if not text_scores:
                continue
            # use the first score on this line
            score = int(text_scores[0])
            if score < 0:
                continue
            if score > 15:
                continue
            text_line = strip_emojis(line)
            parsed_scores.append((
                score,
                parse_text_for_morning(text_line),
                parse_text_for_afternoon(text_line),
                parse_text_for_yesterday(text_line)
            ))
    return parsed_scores


def add_stuff_quiz(stuff_quiz):
    with Database(DATABASE_NAME) as db:
        db.add_quiz(stuff_quiz.id, stuff_quiz.name, stuff_quiz.url, stuff_quiz.ts)


def get_stuff_quiz_by_id(stuff_quiz_id):
    with Database(DATABASE_NAME) as db:
        return db.get_quiz_by_id(stuff_quiz_id)


def try_add_quiz_score(user_id, channel_id, score, is_am, is_pm, is_yesterday, ts):
    with Database(DATABASE_NAME) as db:
        # get the quiz this score is for
        quiz = db.find_quiz(ts, is_am, is_pm, is_yesterday)
        if not quiz:
            return 'quiz could not be found'
        # check if a score has not already been added
        existing_score = db.find_quiz_score(user_id, quiz[0])
        if existing_score is not None:
            return f'already added score `{existing_score}` for this quiz'
        # store score
        db.add_score(user_id, quiz[0], channel_id, score, ts)
        return None


def alert_channel_about_new_stuff_quiz(stuff_quiz, web_client):
    web_client.chat_postMessage(
        channel=QUIZ_CHANNEL,
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"new quiz :sparkles: <{stuff_quiz.url}|{stuff_quiz.name}>"
                }
            }
        ]
    )


def on_new_stuff_quiz(stuff_quiz, web_client):
    # check the day of week
    quiz_ts_datetime = datetime.datetime.fromtimestamp(float(stuff_quiz.ts))
    weekday = quiz_ts_datetime.weekday()
    if weekday not in QUIZ_DAYS_OF_WEEK:
        return
    # see if the quiz already exists in the db
    existing_stuff_quiz = get_stuff_quiz_by_id(stuff_quiz.id)
    if existing_stuff_quiz:
        return
    print(f'new stuff quiz: {stuff_quiz.name}')
    # add quiz to db
    add_stuff_quiz(stuff_quiz)
    # send message
    alert_channel_about_new_stuff_quiz(stuff_quiz, web_client)


def get_leaderboard_block_all_time(leaderboard):
    # split the leaderboard into 2 columns
    # if the number of users is odd, put the extra entry in the first column
    midpoint = int(math.ceil(len(leaderboard) / 2))
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "Average all-time scores:"
        },
        "fields": [
            {
                "type": "mrkdwn",
                "text": "\n".join(
                    (
                        f"*{index + 1}*. {line['name']} "
                        f"`{line['average_score']:.1f}` "
                        f"_({line['total_quizzes']} quiz{'' if line['total_quizzes'] == 1 else 'zes'})_"
                    )
                    for index, line in enumerate(leaderboard[:midpoint])
                )
            },
            {
                "type": "mrkdwn",
                "text": "\n".join(
                    (
                        f"*{index + 1 + midpoint}*. {line['name']} "
                        f"`{line['average_score']:.1f}` "
                        f"_({line['total_quizzes']} quiz{'' if line['total_quizzes'] == 1 else 'zes'})_"
                    )
                    for index, line in enumerate(leaderboard[midpoint:])
                )
            }
        ]
    }


def get_leaderboard_block(leaderboard):
    # split the leaderboard into 2 columns
    # if the number of users is odd, put the extra entry in the first column
    midpoint = int(math.ceil(len(leaderboard) / 2))
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "Average of recent _(last 10)_ scores:"
        },
        "fields": [
            {
                "type": "mrkdwn",
                "text": "\n".join(
                    (
                        f"*{index + 1}*. {line['name']} "
                        f"`{line['recent_average']:.1f}` "
                        f"`{'+' if line['recent_difference'] >= 0 else ''}{line['recent_difference']:.1f}` "
                    )
                    for index, line in enumerate(leaderboard[:midpoint])
                )
            },
            {
                "type": "mrkdwn",
                "text": "\n".join(
                    (
                        f"*{index + 1 + midpoint}*. {line['name']} "
                        f"`{line['recent_average']:.1f}` "
                        f"`{'+' if line['recent_difference'] >= 0 else ''}{line['recent_difference']:.1f}` "
                    )
                    for index, line in enumerate(leaderboard[midpoint:])
                )
            }
        ]
    }


def get_quiz_stats_blocks(quiz_stats):
    # split into easiest and hardest
    midpoint = int(math.ceil(len(quiz_stats) / 2))
    easiest_quizzes = quiz_stats[:midpoint][:3]
    hardest_quizzes = list(reversed(quiz_stats[midpoint:][-3:]))

    def get_winner_mrkdwn(line):
        if line['is_draw']:
            return 'draw'
        return f"won by {line['win']['user_name']} with `{line['win']['score']}`"

    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join((
                    f"Top {len(hardest_quizzes)} hardest quiz{'' if len(hardest_quizzes) == 1 else 'zes'}:",
                    "\n".join(
                        (
                            f"*{index + 1}*. <{line['url']}|{line['name']}> "
                            f"`{line['average_score']:.1f}` "
                            f"_({line['total_scores']} score{'' if line['total_scores'] == 1 else 's'})_ "
                            f"_({get_winner_mrkdwn(line)})_"
                        )
                        for index, line in enumerate(hardest_quizzes)
                    )
                ))
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join((
                    f"Top {len(easiest_quizzes)} easiest quiz{'' if len(easiest_quizzes) == 1 else 'zes'}:",
                    "\n".join(
                        (
                            f"*{index + 1}*. <{line['url']}|{line['name']}> "
                            f"`{line['average_score']:.1f}` "
                            f"_({line['total_scores']} score{'' if line['total_scores'] == 1 else 's'})_ "
                            f"_({get_winner_mrkdwn(line)})_"
                        )
                        for index, line in enumerate(easiest_quizzes)
                    )
                ))
            }
        }
    ]


def get_leaderboard(is_all_time=False):
    with Database(DATABASE_NAME) as db:
        return db.get_leaderboard(is_all_time)


def get_quiz_stats():
    with Database(DATABASE_NAME) as db:
        return db.get_quiz_stats()


def write_leaderboard_to_channel(channel_id, web_client, is_all_time=False):
    t0 = time.time()
    leaderboard = PROCESS_POOL.apply(get_leaderboard, (is_all_time,))
    t1 = time.time()
    print(f'DEBUG got leaderboard from db in {(t1-t0):.2f}s')
    if is_all_time:
        block = get_leaderboard_block_all_time(leaderboard)
    else:
        block = get_leaderboard_block(leaderboard)
    web_client.chat_postMessage(
        channel=channel_id,
        blocks=[block]
    )


def write_quiz_stats_to_channel(channel_id, web_client):
    t0 = time.time()
    quiz_stats = PROCESS_POOL.apply(get_quiz_stats)
    t1 = time.time()
    print(f'DEBUG got quiz stats from db in {(t1-t0):.2f}s')
    blocks = get_quiz_stats_blocks(quiz_stats)
    web_client.chat_postMessage(
        channel=channel_id,
        blocks=blocks
    )


def write_mrkdwn_to_channel(mrkdwn, channel_id, web_client):
    web_client.chat_postMessage(
        channel=channel_id,
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": mrkdwn
                }
            }
        ]
    )


def write_recent_scores_to_channel(channel_id, name_substring, count, web_client):
    with Database(DATABASE_NAME) as db:
        # try and find a single user
        users = db.find_users_by_name_substring(name_substring)
        if len(users) == 0:
            write_mrkdwn_to_channel(
                'No users found',
                channel_id,
                web_client
            )
            return
        elif len(users) > 1:
            write_mrkdwn_to_channel(
                'Multiple users found, please be specific',
                channel_id,
                web_client
            )
            return
        (user_id, user_name) = users[0]
        scores = db.find_recent_scores_by_user_id(user_id, count)
        score_text = ' '.join(f'`{score}`' for score in scores)
        mrkdwn = f"{user_name}'s last {len(scores)} scores: {score_text}"
        write_mrkdwn_to_channel(mrkdwn, channel_id, web_client)


@slack.RTMClient.run_on(event="message")
def message(**payload):
    data = payload["data"]
    web_client = payload["web_client"]
    channel_id = data.get("channel")
    user_id = data.get("user")
    text = data.get("text")
    ts = data.get("ts")

    #print(data)
    #print(web_client)
    #print(channel_id)
    #print(user_id)
    #print(text)

    if not text:
        return

    if text.lower().startswith('!leaderboard'):
        try:
            is_all_time = text.lower().endswith('all-time') or text.lower().endswith('alltime')
            write_leaderboard_to_channel(channel_id, web_client, is_all_time)
        except Exception as e:
            print(f'could not write leaderboard: {e}')
        return

    elif text.lower() == '!quizstats':
        try:
            write_quiz_stats_to_channel(channel_id, web_client)
        except Exception as e:
            print(f'could not write quiz stats: {e}')
        return

    elif text.lower().startswith('!last10 '):
        try:
            name_substring = text[8:]
            write_recent_scores_to_channel(channel_id, name_substring, 10, web_client)
        except Exception as e:
            print(f'could not get last 10 scores: {e}')
        return

    elif text.lower().startswith('!last '):
        try:
            cmd, num, name_substring = text.split()
            int_num = int(num)
            if int_num <= 0:
                add_reaction('dusty_stick', channel_id, ts, web_client)
                return
            elif int_num > 1000:
                write_mrkdwn_to_channel(
                    f'Limit = 1000',
                    channel_id,
                    web_client
                )
                return

            write_recent_scores_to_channel(channel_id, name_substring, int_num, web_client)
        except Exception as e:
            print(f'could not get last scores: {e}')
        return

    # TODO: more commands e.g. personal scores

    try:
        channel_name = get_channel_name(channel_id, web_client)
        if not channel_name:
            return

        # channels might not have hash prefix, so remove for comparison
        if channel_name.lstrip('#') != QUIZ_CHANNEL.lstrip('#'):
            return

        parsed_scores = parse_text_for_scores(text)
        if not parsed_scores:
            return

        # who da perp?
        user_name = get_user_name(user_id, web_client)
        if not user_name:
            return

        # add scores
        for score, is_am, is_pm, is_yesterday in parsed_scores:
            print(f'adding score {score} for user {user_name}')
            error_message = try_add_quiz_score(user_id, channel_id, score, is_am, is_pm, is_yesterday, ts)

            if error_message is not None:
                write_mrkdwn_to_channel(
                    f'Your score `{score}` could not be added: {error_message}',
                    channel_id,
                    web_client
                )
                continue

            # is the score reaction-worthy?
            if score in (0, 1):
                add_reaction('exploding_head', channel_id, ts, web_client)

            elif score in (14, 15):
                add_reaction('fire', channel_id, ts, web_client)

    except Exception as e:
        print(f'exception in message(): {e}')
        return


if __name__ == "__main__":
    # initialize the database
    with Database(DATABASE_NAME) as db:
        db.initialize()

    PROCESS_POOL = Pool(2)

    ssl_context = ssl_lib.create_default_context(cafile=certifi.where())
    slack_token = os.environ["SLACK_BOT_TOKEN"]
    proxy = os.environ.get("PROXY")

    if proxy:
        web_client = slack.WebClient(token=slack_token, ssl=ssl_context, proxy=proxy)
    else:
        web_client = slack.WebClient(token=slack_token, ssl=ssl_context)

    # start the stuff quiz poller
    sq_poller = StuffQuizPoller()
    sq_poller.on_new_stuff_quiz = lambda sq: on_new_stuff_quiz(sq, web_client)
    sq_poller.start()

    while True:
        try:
            if proxy:
                rtm_client = slack.RTMClient(token=slack_token, ssl=ssl_context, proxy=proxy)
            else:
                rtm_client = slack.RTMClient(token=slack_token, ssl=ssl_context)
            rtm_client.start()
        except KeyboardInterrupt:
            print('exiting...')
            break
        except Exception as e:
            print(f'a (slack) exception occurred: {e}')
        # don't reconnect too soon
        time.sleep(30)

    # stop the stuff quiz poller and process pool
    sq_poller.stop()
    PROCESS_POOL.close()

    sq_poller.join()
    PROCESS_POOL.join()
