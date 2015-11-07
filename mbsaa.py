from flask import abort, Flask
from flask.json import jsonify
from subprocess import Popen
from time import sleep

DEBUG = True

app = Flask(__name__)
app.config.from_object(__name__)

PROCESS_DICT = {}


@app.route('/start_spider/<spider_name>')
def start_spider(spider_name):
    dir = '/opt/mbsaa/' + spider_name + '/'
    if spider_name.lower() == 'tripadvisor':
        spider_name = 'ta_spider'
    elif spider_name.lower() == 'skyscraper':
        spider_name = 'ss_spider'
    else:
        return abort(404)
    PROCESS_DICT[spider_name] = Popen(
        ['scrapy', 'crawl', spider_name, '-a', 'config=/opt/mbsaa/config.yml'],
        cwd=dir)
    sleep(10)
    return jsonify(**{'status': PROCESS_DICT[spider_name].poll()})


@app.route('/resume_spider/<spider_name>')
def resume_spider(spider_name):
    dir = '/opt/mbsaa/' + spider_name + '/'
    crawls_dir_param = 'JOBDIR=crawls/' + spider_name
    if spider_name.lower() == 'tripadvisor':
        spider_name = 'ta_spider'
    elif spider_name.lower() == 'skyscraper':
        spider_name = 'ss_spider'
    else:
        return abort(404)
    PROCESS_DICT[spider_name] = Popen(
        ['scrapy', 'crawl', spider_name, '-a', 'config=/opt/mbsaa/config.yml',
         '-s', crawls_dir_param
         ],
        cwd=dir)
    sleep(10)
    return jsonify(**{'status': process.poll()})


@app.route('/stop_crawls/')
def stop_crawls():
    if PROCESS_DICT:
        for process in PROCESS_DICT.keys():
            PROCESS_DICT[process].kill()
        status = 'kill signals sent'
    else:
        status = 'no active crawls found'
    return jsonify(**{'status': status})


if __name__ == '__main__':
    app.run(host='0.0.0.0')
