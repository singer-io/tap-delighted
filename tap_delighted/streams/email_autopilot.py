from tap_delighted.streams.abstracts import IncrementalStream


class EmailAutopilot(IncrementalStream):
    tap_stream_id = "email_autopilot"
    key_properties = ["person_id", "next_survey_request_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    path = "v1/autopilot/email/memberships.json"
    http_method = "GET"

    filter_param = "updated_since"
    is_page_number_pagination = False
