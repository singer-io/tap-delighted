from tap_delighted.streams.abstracts import IncrementalStream


class SurveyResponses(IncrementalStream):
    tap_stream_id = "survey_responses"
    key_properties = "id"
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    path = "/v1/survey_responses.json"
