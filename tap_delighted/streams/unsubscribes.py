from tap_delighted.streams.abstracts import IncrementalStream


class Unsubscribes(IncrementalStream):
    tap_stream_id = "unsubscribes"
    key_properties = ["person_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["unsubscribed_at"]
    path = "v1/unsubscribes.json"
    http_method = "GET"

    is_page_number_pagination = True
    filter_param = "since"
