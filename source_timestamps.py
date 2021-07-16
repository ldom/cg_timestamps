from collections import namedtuple
import sys
import logging

from confluent_kafka import Consumer, KafkaException


GroupTopicPartition = namedtuple('GroupTopicPartition', ['group', 'topic', 'partition'])
GroupTopic = namedtuple('GroupTopic', ['group', 'topic'])

def decode_key(key):
    # longint = size "group" string
    # string = group
    # longint = size "topic" string
    # string = topic
    # longint = partition

    size_group = int.from_bytes(key[0:4], "big")
    end_group = size_group + 4
    group = key[4:end_group]

    size_topic = int.from_bytes(key[end_group:end_group + 4], "big")
    pos_size_topic = end_group + 4
    topic = key[pos_size_topic:pos_size_topic + size_topic]

    pos_partition = pos_size_topic + 4 + len(topic)

    partition = int.from_bytes(key[pos_partition:pos_partition + 4], "big")

    return GroupTopicPartition(group=group.decode("utf-8"), topic=topic.decode("utf-8"), partition=partition)


def decode_value(value):
    # only returns the timestamp (first 8 bytes)
    return int.from_bytes(value[0:8], "big")


def poll_timestamps(conf, consumer_timestamps_topic, callback, filter_group_topic_pairs):
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    c = Consumer(conf, logger=logger)

    c.subscribe([consumer_timestamps_topic, ])

    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                print(f"decoding a message from {msg.topic()}-{msg.partition()} (offset: {msg.offset()}")
                decoded_key = decode_key(msg.key())
                group_topic = GroupTopic(group=decoded_key.group, topic=decoded_key.topic)
                timestamp = decode_value(msg.value())
                print(f"group: {group_topic.group}, topic: {group_topic.topic}, "
                      f"partition: {decoded_key.partition}, timestamp: {timestamp}")

                if group_topic in filter_group_topic_pairs:
                    callback(group_topic.group, group_topic.topic, decoded_key.partition, timestamp)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        c.close()
