FROM python:3.6.1-alpine

WORKDIR /opt/app/
ADD start.sh /opt/app/start.sh

CMD ["./start.sh"]

ADD requirements.txt /opt/app/requirements.txt

RUN apk add --update postgresql-dev alpine-sdk bash ca-certificates wget  \
    && pip install -r /opt/app/requirements.txt \
    && update-ca-certificates

ADD src /opt/app