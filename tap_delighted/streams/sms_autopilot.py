from tap_delighted.streams.abstracts import IncrementalStream


class SmsAutopilot(IncrementalStream):
    tap_stream_id = "sms_autopilot"
    key_properties = []
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    path = "/v1/autopilot/sms/memberships.json"
