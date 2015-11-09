# MBSAA self contained

FROM python:2.7
MAINTAINER CityGlobe

RUN mkdir -p /opt/mbsaa/
COPY ./ /opt/mbsaa/

RUN mkdir -p /etc/supervisor/conf.d
COPY ./supervisord.conf /etc/supervisor/conf.d/
RUN mkdir -p /var/log/supervisor

RUN pip install supervisor --pre
RUN pip install -r /opt/mbsaa/requirements.txt

EXPOSE 5000

# CMD supervisord -c /etc/supervisor/conf.d/supervisord.conf

CMD python /opt/mbsaa/server.py
