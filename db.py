import sqlite3


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
            'CREATE TABLE IF NOT EXISTS scores (user_id string, channel_id string, score integer, ts string);'
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


    def add_score(self, user_id, channel_id, score, ts):
        self._execute(
            'INSERT INTO scores (user_id, channel_id, score, ts) VALUES (?, ?, ?, ?);',
            (user_id, channel_id, score, ts)
        )


    def get_leaderboard(self):
        self._execute(
            'SELECT scores.user_id, users.name, scores.score, scores.ts '
            'FROM scores '
            'JOIN users ON scores.user_id = users.id '
            'ORDER BY scores.ts DESC;'
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
        for user in sorted(users.keys(), key=lambda u: users[u]['recent_average'], reverse=True):
            leaderboard.append(users[user])
        return leaderboard


    def find_users_by_name_substring(self, name_substring):
        self._execute(
            'SELECT id, name FROM users WHERE name LIKE ?;',
            (name_substring, )
        )
        return self.cursor.fetchall()


    def find_recent_scores_by_user_id(self, user_id, count):
        self._execute(
            'SELECT score FROM scores WHERE user_id = ? ORDER BY ts DESC LIMIT ?;',
            (user_id, count)
        )
        rows = self.cursor.fetchall()
        scores = list(row[0] for row in rows)
        return scores
