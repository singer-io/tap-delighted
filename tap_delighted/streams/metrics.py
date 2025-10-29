from tap_delighted.streams.abstracts import FullTableStream


class Metrics(FullTableStream):
    tap_stream_id = "metrics"
    key_properties = []
    replication_method = "FULL_TABLE"
    path = "/v1/metrics.json"
