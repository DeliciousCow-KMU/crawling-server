#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from celery import Celery
from celery.task import Task
import requests
from bs4 import BeautifulSoup

import celeryconfig

app = Celery()
app.config_from_object(celeryconfig)

