import unittest

from source_timestamps import poll_timestamps


class TestMonitor(unittest.TestCase):
    TIMESTAMPS_TOPIC = "__consumer_timestamps"
    REPLICATOR_CG = "replicator"  # replicator's consumer group name
    REPLICATED_TOPIC = "replicated"

    def test_poll(self):

        conf = {'bootstrap.servers': "127.0.0.1:9092",
                'client.id': 'cg_timestamps',
                'group.id': 'cg_timestamps',
                'auto.offset.reset': 'latest'}

        def callback(group, topic, partition, timestamp):
            print(f"group={group}, topic={topic}, partition={partition}, timestamp={timestamp}")

        poll_timestamps(conf, self.TIMESTAMPS_TOPIC, callback, self.REPLICATOR_CG, self.REPLICATED_TOPIC)
