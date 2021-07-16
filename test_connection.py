import argparse
import json
import logging

from confluent_kafka import Consumer


def handle_arguments():
    parser = argparse.ArgumentParser(description="Tests the source connection")

    parser.add_argument("source_connect_config",
                        help="Config properties for connecting to the source cluster, in JSON format. "
                             "Minimum = '{ \"bootstrap.servers\": \"<ip-or-dns-name>:9092\" }'")

    return parser.parse_args()


def read_json_input(filename):
    with open(filename) as f:
        input_data = json.load(f)
        return input_data


if __name__ == '__main__':
    args = handle_arguments()
    conf = read_json_input(args.source_connect_config)

    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    c = Consumer(conf, logger=logger)

    c.close()
