# MBSAA self contained

FROM ubuntu:latest
MAINTAINER CityGlobe

RUN apt-get -y update
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install -y python2.7 python-pip python-dev build-essential libffi-dev libssl-dev libxml2-dev libxslt1-dev

RUN pip install --upgrade pip

COPY ./ .

RUN pip install -r requirements.txt
