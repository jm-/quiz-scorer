import sqlite3
import datetime


class Database():
    def __init__(self, file_name):
        self.file_name = file_name


    def __enter__(self):
        self.conn = sqlite3.connect(self.file_name)
        self.cursor = self.conn.cursor()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()


    def _execute(self, sql, params=None):
        params = params or ()
        self.cursor.execute(sql, params)
        self.conn.commit()


    def initialize(self):
        self._execute(
            'CREATE TABLE IF NOT EXISTS channels (id string, name string);'
        )
        self._execute(
            'CREATE TABLE IF NOT EXISTS users (id string, name string);'
        )
        self._execute(
            'CREATE TABLE IF NOT EXISTS scores (user_id string, quiz_id string, channel_id string, score integer, ts string);'
        )
        self._execute(
            'CREATE TABLE IF NOT EXISTS quizzes (id string, name string, url string, ts string);'
        )


    def get_channel_name_by_id(self, channel_id):
        self._execute(
            'SELECT name FROM channels WHERE id=?;',
            (channel_id,)
        )
        rows = self.cursor.fetchall()
        if len(rows) == 0:
            return None
        return rows[0][0]


    def create_channel(self, channel_id, channel_name):
        self._execute(
            'INSERT INTO channels (id, name) VALUES (?, ?);',
            (channel_id, channel_name)
        )


    def get_user_name_by_id(self, user_id):
        self._execute(
            'SELECT name FROM users WHERE id=?;',
            (user_id,)
        )
        rows = self.cursor.fetchall()
        if len(rows) == 0:
            return None
        return rows[0][0]


    def get_user_id_by_name(self, user_name):
        self._execute(
            'SELECT id FROM users WHERE name=?;',
            (user_name,)
        )
        rows = self.cursor.fetchall()
        if len(rows) == 0:
            return None
        return rows[0][0]


    def create_user(self, user_id, user_name):
        self._execute(
            'INSERT INTO users (id, name) VALUES (?, ?);',
            (user_id, user_name)
        )


    def get_quiz_by_id(self, quiz_id):
        self._execute(
            'SELECT id, name, url, ts '
            'FROM quizzes '
            'WHERE id = ?;',
            (quiz_id,)
        )
        rows = self.cursor.fetchall()
        if rows:
            return rows[0]
        return None


    def add_quiz(self, quiz_id, quiz_name, quiz_url, quiz_ts):
        self._execute(
            'INSERT INTO quizzes (id, name, url, ts) VALUES (?, ?, ?, ?);',
            (quiz_id, quiz_name, quiz_url, quiz_ts)
        )


    def add_score(self, user_id, quiz_id, channel_id, score, ts):
        self._execute(
            'INSERT INTO scores (user_id, quiz_id, channel_id, score, ts) VALUES (?, ?, ?, ?, ?);',
            (user_id, quiz_id, channel_id, score, ts)
        )


    def find_quiz(self, ts, is_am, is_pm, days_ago=0):
        # shift ts to the correct day
        timestamp = float(ts) - (24 * 60 * 60 * days_ago)
        timestamp_date = datetime.datetime.fromtimestamp(timestamp).date()
        midday = datetime.time(12, 0)
        # get quizzes 24h either side of timestamp
        lower = timestamp - (24 * 60 * 60)
        upper = timestamp + (24 * 60 * 60)
        self._execute(
            'SELECT id, name, url, ts '
            'FROM quizzes '
            'WHERE ts > ? AND ts < ? '
            'ORDER BY ts DESC;',
            (lower, upper)
        )
        # return first quiz that matches
        for quiz_id, quiz_name, quiz_url, quiz_ts in self.cursor:
            quiz_ts_datetime = datetime.datetime.fromtimestamp(float(quiz_ts))
            # skip quizzes on different days
            if quiz_ts_datetime.date() != timestamp_date:
                continue
            # match on am/pm/none
            if is_am:
                if quiz_ts_datetime.time() < midday:
                    return (quiz_id, quiz_name, quiz_url, quiz_ts)
            elif is_pm:
                if quiz_ts_datetime.time() >= midday:
                    return (quiz_id, quiz_name, quiz_url, quiz_ts)
            else:
                # return latest quiz
                return (quiz_id, quiz_name, quiz_url, quiz_ts)
        return None


    def find_quiz_score(self, user_id, quiz_id):
        self._execute(
            'SELECT score '
            'FROM scores '
            'WHERE user_id = ? AND quiz_id = ? '
            'ORDER BY ts DESC '
            'LIMIT 1;',
            (user_id, quiz_id)
        )
        rows = self.cursor.fetchall()
        if rows:
            return rows[0][0]
        return None


    def get_leaderboard(self, is_all_time=False):
        # sorted by quiz id (more reliable than time!) then score time
        if is_all_time:
            self._execute(
                'SELECT scores.user_id, users.name, scores.score, scores.ts '
                'FROM scores '
                'JOIN users ON scores.user_id = users.id '
                'LEFT OUTER JOIN quizzes ON scores.quiz_id = quizzes.id '
                'ORDER BY quizzes.ts DESC, scores.ts DESC;'
            )
        else:
            # last 28 days worth of scores only
            cutoff_datetime = datetime.datetime.now().timestamp() - 28 * 24 * 60 * 60
            self._execute(
                'SELECT scores.user_id, users.name, scores.score, scores.ts '
                'FROM scores '
                'JOIN users ON scores.user_id = users.id '
                'LEFT OUTER JOIN quizzes ON scores.quiz_id = quizzes.id '
                'WHERE scores.ts > ? '
                'ORDER BY quizzes.ts DESC, scores.ts DESC;',
                (cutoff_datetime,)
            )

        users = {}
        for row in self.cursor:
            if row[0] not in users:
                users[row[0]] = {
                    'name': row[1],
                    'scores': []
                }
            users[row[0]]['scores'].append(row[2])
        if len(users) == 0:
            return None
        # calculate average and total
        for user in users.keys():
            users[user]['total_quizzes'] = len(users[user]['scores'])
            users[user]['average_score'] = sum(users[user]['scores']) / len(users[user]['scores'])
            users[user]['recent_quizzes'] = len(users[user]['scores'][:10])
            users[user]['recent_average'] = sum(users[user]['scores'][:10]) / len(users[user]['scores'][:10])
            users[user]['recent_1_11_quizzes'] = len(users[user]['scores'][1:11])
            users[user]['recent_1_11_average'] = sum(users[user]['scores'][1:11]) / max(1, len(users[user]['scores'][1:11]))
            # calculate difference
            users[user]['recent_difference'] = users[user]['recent_average'] - users[user]['recent_1_11_average']
        leaderboard = []
        if is_all_time:
            sort_fn = lambda u: users[u]['average_score']
        else:
            sort_fn = lambda u: users[u]['recent_average']
        for user in sorted(users.keys(), key=sort_fn, reverse=True):
            leaderboard.append(users[user])
        return leaderboard


    def get_quiz_stats(self):
        self._execute(
            'SELECT quizzes.id, quizzes.name, quizzes.url, scores.score, users.name '
            'FROM quizzes '
            'JOIN scores ON quizzes.id = scores.quiz_id '
            'JOIN users ON scores.user_id = users.id '
            'ORDER BY quizzes.ts DESC;'
        )
        quizzes = {}
        for row in self.cursor:
            if row[0] not in quizzes:
                quizzes[row[0]] = {
                    'name': row[1],
                    'url': row[2],
                    'scores': [],
                    'win': {
                        'score': -1,
                        'user_name': None
                    },
                    'is_draw': False
                }
            quizzes[row[0]]['scores'].append(row[3])
            # update winner
            if row[3] > quizzes[row[0]]['win']['score']:
                quizzes[row[0]]['win']['score'] = row[3]
                quizzes[row[0]]['win']['user_name'] = row[4]
                quizzes[row[0]]['is_draw'] = False
            elif row[3] == quizzes[row[0]]['win']['score']:
                quizzes[row[0]]['is_draw'] = True
        if len(quizzes) == 0:
            return None
        for quiz_id in quizzes.keys():
            quizzes[quiz_id]['total_scores'] = len(quizzes[quiz_id]['scores'])
            quizzes[quiz_id]['average_score'] = sum(quizzes[quiz_id]['scores']) / len(quizzes[quiz_id]['scores'])
        quiz_stats = []
        for quiz_id in sorted(quizzes.keys(), key=lambda q: quizzes[q]['average_score'], reverse=True):
            quiz = quizzes[quiz_id]
            if len(quiz['scores']) > 1:
                # need to have more than 1 participant to have a winner!
                quiz_stats.append(quiz)
        return quiz_stats


    def find_users_by_name_substring(self, name_substring):
        self._execute(
            'SELECT id, name FROM users WHERE name LIKE ?;',
            (name_substring, )
        )
        return self.cursor.fetchall()


    def find_recent_scores_by_user_id(self, user_id, count):
        self._execute(
            'SELECT scores.score '
            'FROM scores '
            'LEFT OUTER JOIN quizzes ON scores.quiz_id = quizzes.id '
            'WHERE scores.user_id = ? '
            'ORDER BY quizzes.ts DESC, scores.ts DESC '
            'LIMIT ?;',
            (user_id, count)
        )
        rows = self.cursor.fetchall()
        scores = list(row[0] for row in rows)
        return scores
