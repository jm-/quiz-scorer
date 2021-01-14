import os
import re
import time
import datetime
import threading

import urllib3
from bs4 import BeautifulSoup


STUFF_BASE_URL  = 'https://www.stuff.co.nz'
QUIZ_LIST_URL   = STUFF_BASE_URL + '/national/quizzes'
QUIZ_ID_PATTERN = r'/[A-Za-z0-9._-]+/([0-9]+)/'
SLEEP_SECONDS   = 5
SLEEP_TIMES     = 19
CUSTOM_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/87.0.4280.88 '
    'Safari/537.36'
)
QUIZ_POLL_WINDOWS = [
    (datetime.time( 5, 0), datetime.time( 5, 15)),
    (datetime.time(15, 0), datetime.time(15, 15))
]


class StuffQuiz():
    def __init__(self, name, href):
        self.id = re.search(QUIZ_ID_PATTERN, href).group(1)
        self.name = name.strip()
        self.href = href
        self.url = STUFF_BASE_URL + href
        # this requires a GET
        self.ts = None


class StuffQuizPoller(threading.Thread):
    def run(self):
        self.alive = True
        self.force_check = True

        proxy = os.environ.get("PROXY")
        if proxy:
            self.http = urllib3.ProxyManager(proxy)
        else:
            self.http = urllib3.PoolManager()

        while self.alive:
            try:
                if self.should_check_stuff():
                    print('retrieving quizzes from stuff...')
                    stuff_quizzes = self.get_stuff_quizzes()
                    print(f'retrieved {len(stuff_quizzes)} quizzes from stuff')
                    self.process_stuff_quizzes(stuff_quizzes)
            except Exception as e:
                print(f'error getting/processing stuff quizzes: {e}')
            self.sleep()


    def should_check_stuff(self):
        if self.force_check:
            print('forcing a stuff check')
            # only check once
            self.force_check = False
            return True
        current_time = datetime.datetime.now().time()
        for start_time, end_time in QUIZ_POLL_WINDOWS:
            if start_time < current_time < end_time:
                return True
        return False


    def get_stuff_quizzes(self):
        stuff_quizzes = []
        response = self.http.request(
            'GET',
            QUIZ_LIST_URL,
            headers={
                'User-Agent': CUSTOM_USER_AGENT
            }
        )
        soup = BeautifulSoup(response.data, 'html.parser')
        quizzes = soup.select('.main_article h3 a')
        for quiz in quizzes:
            name = quiz.text
            href = quiz.attrs['href']
            stuff_quiz = StuffQuiz(name, href)
            stuff_quizzes.append(stuff_quiz)
        return list(reversed(stuff_quizzes))


    def attach_stuff_quiz_details(self, stuff_quiz):
        # makes a request for more details about this quiz
        response = self.http.request(
            'GET',
            stuff_quiz.url
        )
        soup = BeautifulSoup(response.data, 'html.parser')
        quiz_date = soup.select('.sics-component__byline__date')[0].text
        quiz_ts = time.mktime(time.strptime(quiz_date, '%H:%M, %b %d %Y'))
        stuff_quiz.ts = str(quiz_ts)


    def process_stuff_quizzes(self, stuff_quizzes):
        if hasattr(self, 'on_new_stuff_quiz'):
            for stuff_quiz in stuff_quizzes:
                self.attach_stuff_quiz_details(stuff_quiz)
                self.on_new_stuff_quiz(stuff_quiz)


    def sleep(self):
        i = 0
        while i < SLEEP_TIMES and self.alive and not self.force_check:
            time.sleep(SLEEP_SECONDS)
            i += 1


    def stop(self):
        self.alive = False


if __name__ == '__main__':
    poller = StuffQuizPoller()
    poller.start()
    poller.join()
    print('done!')
