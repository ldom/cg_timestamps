# cg_timestamps

A `python` script to reproduce what Replicator does, when you have it setup to not replicate a topic (but need the consumer timestamps), or to not translate the timestamps of a replicated topic.

The script monitors the `__consumer_timestamps` topic for the consumer group/topic pairs provided in a CSV file and runs the `kafka-consumer-group` command to update the offsets on the other side.
