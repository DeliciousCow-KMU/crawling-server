#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from celery import Celery
from celery.task import Task
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import datetime
import time
import locale

import celeryconfig
import sqlalchemy_session
from models import *

app = Celery()
app.config_from_object(celeryconfig)

app.conf.beat_schedule = {
    'method_trigger': {
        'task': 'app.polling',
        'schedule': 60,
        # 'args': (16, 16)
    },
}

try:
    with open('constant.json', 'r') as f:
        settings = json.load(f)
except:
    raise Exception('Can not found `constant.json` file')

SQL = sqlalchemy_session.DB(settings['sql-host'], settings['sql-user'],
                            settings['sql-pw'], 'crawling')


def _get_local_tz() -> datetime.timezone:
    EPOCH = datetime.datetime(1970, 1, 1)
    OS_ENCODING = locale.getdefaultlocale()[1]

    local_datetime = datetime.datetime(*time.localtime(0)[:6])
    dst = time.daylight and local_datetime.tm_isdst > 0
    gmtoff = -(time.altzone if dst else time.timezone)

    time_delta = local_datetime - EPOCH

    if time_delta == datetime.timedelta(seconds=gmtoff):
        tz_name = time.tzname[dst].encode('charmap').decode(OS_ENCODING)
        tz = datetime.timezone(time_delta, tz_name)
    else:
        tz = datetime.timezone(time_delta)

    return tz


class TimeManager(object):
    TIMEZONE_KR = datetime.timezone(datetime.timedelta(0, 32400), 'Asia/Seoul')
    TIMEZONE_UTC = datetime.timezone.utc
    TIMEZONE_LOCAL = _get_local_tz()

    @staticmethod
    def _get_utc_datetime():
        return datetime.datetime.utcnow(). \
            replace(tzinfo=TimeManager.TIMEZONE_UTC)

    @staticmethod
    def _get_kr_datetime():
        return TimeManager._get_utc_datetime(). \
            astimezone(TimeManager.TIMEZONE_KR)

    @staticmethod
    def get_now_datetime(utc=False) -> datetime.datetime:
        return TimeManager._get_utc_datetime() if utc else TimeManager._get_kr_datetime()

    @staticmethod
    def formatted_today(utc=False) -> str:
        dt = TimeManager._get_utc_datetime() if utc else TimeManager._get_kr_datetime()
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def formatted_now(utc=False) -> str:
        dt = TimeManager._get_utc_datetime() if utc else TimeManager._get_kr_datetime()
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def str_to_timestamp(formatted_str: str, utc=False) -> int:
        dt = TimeManager.str_to_datetime(formatted_str, utc)
        timetuple = dt.timetuple()
        return time.mktime(timetuple)

    @staticmethod
    def str_to_datetime(formatted_str: str, utc=False) -> datetime.datetime:
        tz = TimeManager.TIMEZONE_UTC if utc else TimeManager.TIMEZONE_KR
        return datetime.datetime. \
            strptime(formatted_str, "%Y-%m-%d %H:%M:%S"). \
            replace(tzinfo=tz)

    @staticmethod
    def timestamp_to_datetime(timestamp: int) -> datetime.datetime:
        return datetime.datetime. \
            fromtimestamp(timestamp, TimeManager.TIMEZONE_LOCAL)

    @staticmethod
    def datetime_to_timestamp(dt: datetime.datetime, from_tz: datetime.timezone) -> int:
        return int(
            time.mktime(
                dt.replace(tzinfo=from_tz).
                    astimezone(TimeManager.TIMEZONE_LOCAL).
                    timetuple()
            )
        )

    @staticmethod
    def to_KR(dt: datetime.datetime) -> datetime.datetime:
        return dt.astimezone(TimeManager.TIMEZONE_KR)

    @staticmethod
    def to_UTC(dt: datetime.datetime) -> datetime.datetime:
        return dt.astimezone(TimeManager.TIMEZONE_UTC)


@app.task
def polling():
    notice_list = crawling_CS_notice_list()
    for i in notice_list:
        with SQL.get_session() as session:
            if not session.query(Post) \
                    .filter(Post.post_id == i['post_id']) \
                    .filter(Post.division == '소프트웨어융합대학') \
                    .first():
                crawling_CS_article.delay(i)


def crawling_CS_notice_list():
    results = []
    url = 'https://cs.kookmin.ac.kr/news/notice/'
    with requests.get(url) as response:
        html = response.text

    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find('div', {'class': 'table-wrap'})
    if table:
        tbody = table.find('div', {'class': 'list-tbody'})
        if tbody:
            for item in tbody.find_all('ul'):
                data = dict(url=None, important=False, post_id=None, division='소프트웨어융합대학')

                if 'notice-bg' in item.attrs.get('class'):
                    data['important'] = True

                subject = item.find('li', {'class': 'subject'})
                if subject:
                    a_tag = subject.find('a')
                    href = a_tag.get('href')
                    data['post_id'] = href[2:]
                    data['url'] = urljoin(url, href)

                results.append(data)

    return results


@app.task
def crawling_CS_article(data):
    url = data.get('url')
    with requests.get(url) as response:
        html = response.text

    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find('table', {'class': 'article-info-type'})
    trs = table.find_all('tr')

    data['title'] = trs[0].find('td', {'class': 'view-title'}).text
    data['date'], data['department'], data['author'] = (td.text for td in trs[1].find_all('td'))
    data['text'] = trs[3].find('div', {'id': 'view-detail-data'}).text.replace('\xa0', ' ').strip()
    data['date'] = datetime.datetime.strptime(data['date'], '%y.%m.%d')
    data['find_at'] = TimeManager.get_now_datetime()

    return insert_post_data(data)


def insert_post_data(data: dict):
    with SQL.get_session() as session:
        model = Post(title=data['title'], post_id=data['post_id'], department=data['department'], author=data['author'],
                     text=data['text'], find_at=data['find_at'], date=data['date'],
                     important=data.get('important', False), url=data['url'], division=data['division'])
        session.add(model)
        session.commit()


def _create_db():
    """
    It will called just once when creating the DB table.
    """
    engine = SQL.get_engine('crawling')
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    _create_db()
