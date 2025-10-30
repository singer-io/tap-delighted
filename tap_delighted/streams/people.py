from tap_delighted.streams.abstracts import IncrementalStream


class People(IncrementalStream):
    tap_stream_id = "people"
    key_properties = "id"
    replication_method = "INCREMENTAL"
    replication_keys = ["created_at"]
    path = "v1/people.json"
