from tap_delighted.streams.abstracts import IncrementalStream


class EmailAutopilot(IncrementalStream):
    tap_stream_id = "email_autopilot"
    key_properties = []
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    path = "v1/autopilot/email/memberships.json"
