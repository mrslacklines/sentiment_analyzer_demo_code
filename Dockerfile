# MBSAA self contained

FROM python:2.7
MAINTAINER CityGlobe

RUN mkdir -p /opt/mbsaa/aggregator
COPY ./ /opt/mbsaa/aggregator

RUN mkdir -p /etc/supervisor/conf.d
COPY ./supervisord.conf /etc/supervisor/conf.d/
RUN mkdir -p /var/log/supervisor

RUN pip install supervisor --pre
RUN pip install -r /opt/mbsaa/aggregator/requirements.txt

CMD supervisord -c /etc/supervisor/conf.d/supervisord.conf
