FROM python:3.6.1-alpine

WORKDIR /opt/app/

CMD ["./start.sh"]

ADD requirements.txt /opt/app/requirements.txt

RUN apk add --update postgresql-dev alpine-sdk bash ca-certificates git wget libxml2-dev libxslt-dev \
     && pip install -r /opt/app/requirements.txt \
    && pip install git+https://github.com/alberanid/imdbpy \
    && update-ca-certificates

RUN git clone https://github.com/alberanid/imdbpy.git

ADD start.sh /opt/app/start.sh