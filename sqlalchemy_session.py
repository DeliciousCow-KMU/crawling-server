# coding=utf-8
import traceback
import sys

from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm.scoping import scoped_session

_db_schema = None
try:
    import pymysql
    _db_schema = 'mysql+pymysql://'
except:
    import _mysql
    _db_schema = 'mysql+mysqldb://'

class Session:
    def __init__(self, engine, auto=False):
        self.engine = engine
        self.auto = auto

    def __enter__(self):
        self.sess = scoped_session(
            sessionmaker(
                autocommit=self.auto, autoflush=self.auto, bind=self.engine)
        )
        return self.sess

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sess.close()


class DB:
    def __init__(self, host, user, password, default_db):
        self._engine = {}
        self.host = host
        self.auth = user, password
        self.default_db = default_db


    def _create_engine(self, db):
        try:
            engine = create_engine(
                _db_schema + '%s:%s@%s/%s?charset=utf8' % (self.auth[0], self.auth[1], self.host, db),
                pool_recycle=500,
                pool_size=10,
                max_overflow = 5
            )
            conn = engine.connect()
            conn.execute('select 1')
            conn.close()
            return engine

        except OperationalError:
            print('Failed to connect db')
            traceback.print_exc()
            sys.exit(1)

        except:
            print('cannot find mysql client')
            sys.exit(1)

    def get_engine(self, db=None):
        if not db:
            db = self.default_db

        if db not in self._engine:
            self._engine[db] = self._create_engine(db)

        return self._engine[db]

    def get_session(self, db=None, auto_commit=False):
        return Session(engine=self.get_engine(db), auto=auto_commit)