# coding=utf-8
from sqlalchemy import Column, String, Text, Boolean, Integer, DateTime, Time
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True)
    title = Column(Text)
    post_id = Column(Integer)
    department = Column(Text)
    author = Column(Text)
    text = Column(Text)
    find_at = Column(DateTime)
    date = Column(DateTime)
    url = Column(Text)
    division = Column(Text)
    important = Column(Boolean, default=False)
