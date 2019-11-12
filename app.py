import os
import re
import logging
import slack
import ssl as ssl_lib
import certifi
import threading

from db import Database
from stuffquiz import StuffQuiz, StuffQuizPoller


QUIZ_CHANNEL    = '#quizscores'
DATABASE_NAME   = 'quiz-scorer.db'


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
    web_client.reactions_add(
        name=name,
        channel=channel_id,
        timestamp=ts
    )


def parse_text_for_score(text):
    try:
        match = re.search(r'([0-9]{1,2})/15', text)
    except Exception as e:
        print(f'exception parsing text for score: {e} ({text})')
        return None

    if not match:
        return None
    score = int(match.group(1))
    if score < 0:
        return None
    if score > 15:
        return None
    return score


def add_quiz_score(user_id, channel_id, score, ts):
    with Database(DATABASE_NAME) as db:
        db.add_score(user_id, channel_id, score, ts)


def alert_channel_about_new_stuff_quiz(stuff_quiz, web_client):
    web_client.chat_postMessage(
        channel=QUIZ_CHANNEL,
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"new quiz :tada: <{stuff_quiz.url}|{stuff_quiz.name}>"
                }
            }
        ]
    )


def get_leaderboard_block(leaderboard):
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "\n".join(
                f"{line['name']} `{line['average_score']:.1f}` _({line['total_quizzes']} quiz{'' if line['total_quizzes'] == 1 else 'zes'})_"
                for line in leaderboard
            )
        }
    }


def write_leaderboard_to_channel(channel_id, web_client):
    with Database(DATABASE_NAME) as db:
        leaderboard = db.get_leaderboard()
    block = get_leaderboard_block(leaderboard)
    web_client.chat_postMessage(
        channel=channel_id,
        blocks=[block]
    )


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

    if text == '!leaderboard':
        try:
            write_leaderboard_to_channel(channel_id, web_client)
        except Exception as e:
            print(f'could not write leaderboard: {e}')
        return

    # TODO: more commands e.g. personal scores

    try:
        channel_name = get_channel_name(channel_id, web_client)
        if not channel_name:
            return

        # channels might not have hash prefix, so remove for comparison
        if channel_name.lstrip('#') != QUIZ_CHANNEL.lstrip('#'):
            return

        score = parse_text_for_score(text)
        if score is None:
            return

        # who da perp?
        user_name = get_user_name(user_id, web_client)
        if not user_name:
            return

        # add score
        print(f'adding score {score} for user {user_name}')
        add_quiz_score(user_id, channel_id, score, ts)

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

    ssl_context = ssl_lib.create_default_context(cafile=certifi.where())
    slack_token = os.environ["SLACK_BOT_TOKEN"]
    proxy = os.environ["PROXY"]

    web_client = slack.WebClient(token=slack_token, ssl=ssl_context, proxy=proxy)
    rtm_client = slack.RTMClient(token=slack_token, ssl=ssl_context, proxy=proxy)

    # start the stuff quiz poller
    sq_poller = StuffQuizPoller()
    sq_poller.on_new_stuff_quiz = lambda sq: alert_channel_about_new_stuff_quiz(sq, web_client)
    sq_poller.start()

    rtm_client.start()
