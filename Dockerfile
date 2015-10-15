FROM ubuntu:latest

RUN apt-get -y update
RUN apt-get -y install -y python2.7 python-pip python-dev build-essential
RUN pip install --upgrade pip
RUN pip install requests[security]

RUN pip install -r requirements.txt
