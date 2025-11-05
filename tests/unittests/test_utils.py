import unittest
from unittest.mock import MagicMock

from parameterized import parameterized

from tap_delighted.utils import (DelightedPaginator, get_datetime_fields_from_schema,
                                 get_datetime_from_timestamp,
                                 get_timestamp_from_datetime,
                                 normalize_autopilot_record)

STANDARD_TEST_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {
            "type": [
                "null",
                "integer"
            ]
        },
        "created_at": {
            "type": [
                "null",
                "string"
            ],
            "format": "date-time"
        },
        "updated_at": {
            "type": [
                "null",
                "string"
            ],
            "format": "date-time"
        }
    }
}

NESTED_TEST_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {
            "type": [
                "null",
                "integer"
            ]
        },
        "profile": {
            "type": [
                "null",
                "object"
            ],
            "properties": {
                "birth_date": {
                    "type": [
                        "null",
                        "string"
                    ],
                    "format": "date-time"
                },
                "signup_date": {
                    "type": [
                        "null",
                        "string"
                    ],
                    "format": "date-time"
                }
            }
        },
        "events": {
            "type": [
                "null",
                "array"
            ],
            "items": {
                "type": [
                    "null",
                    "object"
                ],
                "properties": {
                    "event_date": {
                        "type": [
                            "null",
                            "string"
                        ],
                        "format": "date-time"
                    },
                    "event_type": {
                        "type": [
                            "null",
                            "string"
                        ]
                    }
                }
            }
        }
    }
}

NESTED_ARRAY_TEST_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {
            "type": [
                "null",
                "integer"
            ]
        },
        "activities": {
            "type": [
                "null",
                "array"
            ],
            "items": {
                "type": [
                    "null",
                    "object"
                ],
                "properties": {
                    "activity_date": {
                        "type": [
                            "null",
                            "string"
                        ],
                        "format": "date-time"
                    },
                    "details": {
                        "type": [
                            "null",
                            "object"
                        ],
                        "properties": {
                            "last_updated": {
                                "type": [
                                    "null",
                                    "string"
                                ],
                                "format": "date-time"
                            }
                        }
                    }
                }
            }
        }
    }
}

AUTOPILOT_KEY_PROPERTIES = ["person_id", "next_survey_request_id"]

AUTOPILOT_INPUT_RECORD = {
    "created_at": 1756905076,
    "updated_at": 1761052276,
    "person": {
        "id": "1",
        "email": "test@mail.com",
    },
    "next_survey_request": {
        "id": "4",
        "created_at": 1756905076,
        "survey_scheduled_at": 1763125876
    }
}

AUTOPILOT_OUTPUT_RECORD = {
    "created_at": 1756905076,
    "updated_at": 1761052276,
    "person_id": "1",
    "next_survey_request_id": "4",
    "person": {
        "id": "1",
        "email": "test@mail.com",
    },
    "next_survey_request": {
        "id": "4",
        "created_at": 1756905076,
        "survey_scheduled_at": 1763125876
    }
}


class TestUtils(unittest.TestCase):

    @parameterized.expand([
        ["none value", None, None],
        ["valid timestamp", 1625077800, "2021-06-30T18:30:00Z"],
    ])
    def test_datetime_from_timestamp(self, test_name, timestamp, expected):
        """Test the get_datetime_from_timestamp function."""

        result = get_datetime_from_timestamp(timestamp)
        self.assertEqual(result, expected)

    @parameterized.expand([
        ["null value", None, None],
        ["valid datetime", "2021-06-30T18:30:00Z", 1625077800],
        ["valid bookmark datetime", "2021-12-01T10:15:30.000000Z", 1638353730],
    ])
    def test_get_timestamp_from_datetime(self, test_name, datetime_str, expected):
        """Test the get_timestamp_from_datetime function."""

        result = get_timestamp_from_datetime(datetime_str)
        self.assertEqual(result, expected)

    @parameterized.expand([
        ["standard schema", STANDARD_TEST_SCHEMA, {"created_at", "updated_at"}],
        ["nested schema", NESTED_TEST_SCHEMA, {"birth_date", "signup_date", "event_date"}],
        ["nested array schema", NESTED_ARRAY_TEST_SCHEMA, {"activity_date", "last_updated"}],
    ])
    def test_get_datetime_fields_from_schema(self, test_name, schema, expected_datetime_fields):
        """Test the get_datetime_fields_from_schema function. This test will check various schema structures."""

        actual_datetime_fields = get_datetime_fields_from_schema(schema)
        self.assertEqual(actual_datetime_fields, expected_datetime_fields)

    @parameterized.expand([
        ["without data", [], {}, {}],
        ["without key properties", [], AUTOPILOT_INPUT_RECORD, AUTOPILOT_INPUT_RECORD],
        ["with key properties", AUTOPILOT_KEY_PROPERTIES, AUTOPILOT_INPUT_RECORD, AUTOPILOT_OUTPUT_RECORD],
    ])
    def test_normalize_autopilot_record(self, test_name, key_properties, input_data, expected_data):
        """Test the normalize_autopilot_record function.
        This test will check if key properties are moved to root level correctly in different scenarios.
        """
        normalize_autopilot_record(input_data, key_properties)

        self.assertEqual(input_data, expected_data)


class TestDelightedPaginator(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.params = {"per_page": 50, "page": 1}
        self.http_method = "GET"
        self.paginator = DelightedPaginator(client=self.client, params=self.params,
                                            http_method=self.http_method, url_endpoint="/surveys")

    def test_get_params(self):

        self.assertEqual(self.paginator._get_params, self.params)

    def test_page_number_pagination(self):
        """ Test page number based pagination """

        # Mock 3 pages of API data
        mock_responses = [
            {"data": [{"id": 1}, {"id": 2}]},  # Page 1
            {"data": [{"id": 3}]},             # Page 2
            {"data": []},                      # Page 3 (empty = stop)
        ]

        # Use side effect so that each call returns the response of each page
        self.client.make_request.side_effect = mock_responses

        # Consume the generator all at once to get all fetched records
        all_records = list(self.paginator._page_number_pagination())

        assert all_records == [{"id": 1}, {"id": 2}, {"id": 3}]

        # Confirm 3 pages were fetched by checking call count
        assert self.client.make_request.call_count == 3

        # Confirm the last page is 3 for which no record was found from the paginator's params
        assert self.paginator._get_params["page"] == 3

    def test_cursor_pagination(self):
        """ Test cursor based pagination """

        # Make 2 response objects
        resp1 = MagicMock()
        resp1.json.return_value = [{"key1": "val1"}]
        resp1.headers = {"Link": '<https://api.delighted.com/v1/autopilot/?page=2>; rel="next"'}

        resp2 = MagicMock()
        resp2.json.return_value = [{"key2": "val2"}]
        resp2.headers = {}

        mock_responses = [resp1, resp2]

        # Use side effect so that each call returns the response of each page
        self.client.make_request.side_effect = mock_responses

        # Consume the generator all at once to get all fetched records
        all_records = list(self.paginator._cursor_pagination())

        assert all_records == [{"key1": "val1"}, {"key2": "val2"}]

        # Confirm 2 pages were fetched by checking call count
        assert self.client.make_request.call_count == 2
