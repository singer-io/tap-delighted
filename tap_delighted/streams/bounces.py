from tap_delighted.streams.abstracts import IncrementalStream


class Bounces(IncrementalStream):
    tap_stream_id = "bounces"
    key_properties = "person_id"
    replication_method = "INCREMENTAL"
    replication_keys = ["bounced_at"]
    path = "v1/bounces.json"
