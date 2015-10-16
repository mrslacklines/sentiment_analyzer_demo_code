# MBSAA self contained

FROM ubuntu:latest
MAINTAINER CityGlobe

RUN mkdir -p /opt/mbsaa/aggregator
COPY ./ /opt/mbsaa/aggregator

RUN apt-get -y update
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install -y python2.7 python-pip python-dev build-essential libffi-dev libssl-dev libxml2-dev libxslt1-dev

RUN pip install --upgrade pip
RUN pip install --upgrade requests[security]
RUN pip install supervisor --pre
RUN pip install -r /opt/mbsaa/aggregator/requirements.txt
