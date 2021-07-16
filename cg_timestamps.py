import argparse
from collections import namedtuple
from datetime import datetime
import json

from commands import run_command
from source_timestamps import poll_timestamps, GroupTopic


def handle_arguments():
    parser = argparse.ArgumentParser(description="Takes a list of topics, monitors the __consumer_timestamps topic"
                                                 "on the source cluster and runs commands to set the offsets of the "
                                                 "consumer groups on the destination cluster "
                                                 "to the last read on the source. Basically it does what Replicator "
                                                 "does when translating offsets.")

    parser.add_argument("topics", help="CSV file containing all group and topic names (one per line, "
                                       "format = 'group;topic')")

    parser.add_argument("source_connect_config",
                        help="Config properties for connecting to the source cluster, in JSON format. "
                             "Minimum = '{ \"bootstrap.servers\": \"<ip-or-dns-name>:9092\" }'")

    parser.add_argument("command_template",
                        help="Template file for the 'kafka-consumer-groups' command on the destination cluster.")

    return parser.parse_args()


def read_json_input(filename):
    with open(filename) as f:
        input_data = json.load(f)
        return input_data


def read_command_template(filename):
    with open(filename) as f:
        return f.read()


def read_group_topic_pairs(filename):
    with open(filename) as f:
        data = f.read()
    output = []
    for l in data.splitlines():
        parts = l.split(';')
        group = parts[0].strip()
        topic = parts[1].strip()
        output.append(GroupTopic(group=group, topic=topic))
    return output


def timestamp_to_cg_datetime(timestamp):
    # --to-datetime Reset offsets to offset from datetime. Format: 'YYYY-MM-DDTHH:mm:SS.sss'

    # what we get is a timestamp in milliseconds and python wants a float anyway
    timestamp_in_seconds = timestamp / 1000
    dt = datetime.fromtimestamp(timestamp_in_seconds)
    return dt.isoformat(timespec='milliseconds')


if __name__ == '__main__':
    args = handle_arguments()
    command_template = read_command_template(args.command_template)

    GROUP_PLACEHOLDER = "{{GROUP}}"
    TOPIC_PLACEHOLDER = "{{TOPIC}}"
    DATETIME_PLACEHOLDER = "{{DATETIME}}"

    TIMESTAMPS_TOPIC = "__consumer_timestamps"

    conf = read_json_input(args.source_connect_config)
    group_topic_pairs = read_group_topic_pairs(args.topics)

    def callback(group, topic, partition, timestamp):
        print(f"group={group}, topic={topic}, partition={partition}, timestamp={timestamp}")

        dt = timestamp_to_cg_datetime(timestamp)
        topic_partition = f"{topic}:{partition}"

        command = command_template.\
            replace(GROUP_PLACEHOLDER, group).\
            replace(TOPIC_PLACEHOLDER, topic_partition).\
            replace(DATETIME_PLACEHOLDER, dt)

        print(run_command(command))

    poll_timestamps(conf, TIMESTAMPS_TOPIC, callback, group_topic_pairs)
