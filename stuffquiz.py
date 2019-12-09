import os
import time
import threading

import urllib3
from bs4 import BeautifulSoup


STUFF_BASE_URL  = 'https://www.stuff.co.nz'
QUIZ_LIST_URL   = STUFF_BASE_URL + '/national/quizzes'
SLEEP_SECONDS   = 5
SLEEP_TIMES     = 19


class StuffQuiz():
    def __init__(self, name, href):
        self.name = name.strip()
        self.href = href
        self.url = STUFF_BASE_URL + href


class StuffQuizPoller(threading.Thread):
    def run(self):
        self.alive = True
        self.initialize()
        self.sleep()
        while self.alive:
            try:
                stuff_quizzes = self.get_stuff_quizzes()
            except Exception as e:
                print(f'error getting stuff quizzes: {e}')
            else:
                self.process_stuff_quizzes(stuff_quizzes)
            self.sleep()


    def initialize(self):
        self.stuff_quiz_urls = set()
        stuff_quizzes = self.get_stuff_quizzes()
        for stuff_quiz in stuff_quizzes:
            self.stuff_quiz_urls.add(stuff_quiz.url)
        print(f'loaded {len(stuff_quizzes)} stuff quizzes')


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


    def process_stuff_quizzes(self, stuff_quizzes):
        for stuff_quiz in stuff_quizzes:
            if stuff_quiz.url in self.stuff_quiz_urls:
                continue
            # this is a new quiz!
            print(f'new stuff quiz: {stuff_quiz.name}')
            if self.on_new_stuff_quiz:
                self.on_new_stuff_quiz(stuff_quiz)
            # add url
            self.stuff_quiz_urls.add(stuff_quiz.url)


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
