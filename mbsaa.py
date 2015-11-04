from flask import (
    abort, flash, Flask, g, redirect, render_template, request, session,
    url_for
)
from redis import Redis

DATABASE = '/tmp/flaskr.db'
DEBUG = True
SECRET_KEY = 'development key'
USERNAME = 'admin'
PASSWORD = 'default'

app = Flask(__name__)
app.config.from_object(__name__)


def connect_db(db):
    Redis(host='redis', db=db)

if __name__ == '__main__':
    app.run()
