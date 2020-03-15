import os
import re
import time
import threading

import urllib3
from bs4 import BeautifulSoup


STUFF_BASE_URL  = 'https://www.stuff.co.nz'
QUIZ_LIST_URL   = STUFF_BASE_URL + '/national/quizzes'
QUIZ_ID_PATTERN = r'/[A-Za-z0-9._-]+/([0-9]+)/'
SLEEP_SECONDS   = 5
SLEEP_TIMES     = 19


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
        while self.alive:
            try:
                stuff_quizzes = self.get_stuff_quizzes()
            except Exception as e:
                print(f'error getting stuff quizzes: {e}')
            else:
                self.process_stuff_quizzes(stuff_quizzes)
            self.sleep()


    def get_stuff_quizzes(self):
        stuff_quizzes = []
        http = urllib3.ProxyManager(os.environ["PROXY"])
        response = http.request(
            'GET',
            QUIZ_LIST_URL
        )
        soup = BeautifulSoup(response.data, 'html.parser')
        quizzes = soup.select('.main_article h3 a')
        for quiz in quizzes:
            name = quiz.text
            href = quiz.attrs['href']
            stuff_quiz = StuffQuiz(name, href)
            stuff_quizzes.append(stuff_quiz)
        return stuff_quizzes


    def attach_stuff_quiz_details(self, stuff_quiz):
        # makes a request for more details about this quiz
        http = urllib3.ProxyManager(os.environ["PROXY"])
        response = http.request(
            'GET',
            stuff_quiz.url
        )
        soup = BeautifulSoup(response.data, 'html.parser')
        quiz_date = soup.select('.sics-component__byline__date')[0].text
        quiz_ts = time.mktime(time.strptime(quiz_date, '%H:%M, %b %d %Y'))
        stuff_quiz.ts = str(quiz_ts)


    def process_stuff_quizzes(self, stuff_quizzes):
        for stuff_quiz in stuff_quizzes:
            if hasattr(self, 'on_new_stuff_quiz'):
                self.attach_stuff_quiz_details(stuff_quiz)
                self.on_new_stuff_quiz(stuff_quiz)


    def sleep(self):
        i = 0
        while i < SLEEP_TIMES and self.alive:
            time.sleep(SLEEP_SECONDS)
            i += 1


    def stop(self):
        self.alive = False


if __name__ == '__main__':
    poller = StuffQuizPoller()
    poller.start()
    poller.join()
    print('done!')
