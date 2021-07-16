FROM python:3.9-alpine

WORKDIR /home

RUN apk add --no-cache alpine-sdk librdkafka librdkafka-dev

RUN cd /home && python -V \
    && pip install --upgrade pip \
    && pip install confluent-kafka

#    && pip install --no-binary :all: confluent-kafka # if building from source

COPY ./* /home/

ENTRYPOINT ["python3", "/home/cg_timestamps.py", "groups_topics.csv", "source_conf.json", "command.tpl"]
