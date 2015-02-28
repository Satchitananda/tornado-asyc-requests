# coding=utf-8
import random
import momoko
import datetime

from random import randint

from tornado import httpserver, ioloop, web, gen

DATA_CHOICES = {
    'professions': [
        'web developer',
        'doctor',
        'teacher',
        'engineer',
        'entrepreneur'
    ],
    'names': [
        'Ivan',
        'Sergey',
        'Nina',
        'Zina',
        'Anna',
        'Oxana',
        'Vladimir',
        'Alexander'
    ],
    'hobbies': [
        'guitar',
        'games',
        'snowboard',
        'ski',
        'reading books'
    ],
}


class SetupHandler(web.RequestHandler):
    def get(self):
        sql = """
            CREATE TABLE IF NOT EXISTS test_data (
                id SERIAL PRIMARY KEY,
                name  varchar(80),
                date_born date,
                profession  varchar(80),
                hobby  varchar(80)
            );
        """
        result = self.application.db.execute(sql)
        self.write(u'Ладушки')
        return result


class GetHandler(web.RequestHandler):
    @gen.coroutine
    def get(self, id):
        simple = """
            SELECT %s;
            SELECT id, name, date_born, profession, hobby
            FROM test_data LIMIT 10
        """
        heavy = """
            SELECT %s;
            SELECT id, name, date_born, profession, hobby
            FROM test_data ORDER BY random() LIMIT 1000
        """
        print 'Start %s' % id
        params = [(random.choice([heavy, simple]), i) for i in xrange(100)]
        yield list(map(lambda p: self.make_request(*p), params))
        print 'Finish %s' % id
        self.finish()

    @gen.coroutine
    def make_request(self, sql, number):
        cursor = yield momoko.Op(self.application.db.execute, sql, (number,))
        self.write(u"Получен результат выполнения запроса: %s<br/>" % cursor.query)
        self.flush()

class SetHandler(web.RequestHandler):
    @gen.coroutine
    def post(self):
        sql = """
            INSERT INTO test_data (name, date_born, profession, hobby)
            VALUES (%s, %s, %s, %s)
        """
        name = random.choice(DATA_CHOICES['names'])
        profession = random.choice(DATA_CHOICES['professions'])
        hobby = random.choice(DATA_CHOICES['hobbies'])
        date_born = datetime.datetime.now() - datetime.timedelta(days=randint(1, 365) * randint(18, 35))
        yield momoko.Op(self.application.db.execute, sql, (name, date_born, profession, hobby))
        self.write(u'Ладушки')
        self.finish()

class Application(web.Application):
    def __init__(self):
        handlers = [
            (r"/(.*)/",  GetHandler),
            (r"/set/",   SetHandler),
            (r"/setup/", SetupHandler),
        ]
        super(Application, self).__init__(handlers, debug=True)
        dsn = "dbname=test user=test password=test host=localhost port=5432"
        self.db = momoko.Pool(dsn=dsn, size=5)


if __name__ == "__main__":
    server = httpserver.HTTPServer(Application())
    server.listen(8080)
    ioloop.IOLoop.instance().start()