import unittest
from typing import Dict, Set
from unittest.mock import MagicMock, patch

from parameterized import parameterized

from tap_delighted.streams.abstracts import IncrementalStream
from tap_delighted.utils import get_datetime_from_timestamp

DATETIME_FIELDS = {"created_at", "updated_at", "signup_date", "timestamp"}

RECORD_WITH_TIMESTAMP = {
    "id": 1,
    "created_at": 1672579200,
    "updated_at": 1672579201
}

NESTED_DICT_RECORD_WITH_TIMESTAMP = {
    "id": 1,
    "profile": {
        "name": "John Doe",
        "signup_date": 1672579202,
        "account": {
            "id": 10,
            "phone": "123-456-7890",
            "created_at": 1672579203,
            "updated_at": 1672579204
        }
    },
    "created_at": 1672579205,
    "updated_at": 1672579206
}

NESTED_ARRAY_RECORD_WITH_TIMESTAMP = {
    "id": 1,
    "profile": {
        "name": "Jane Smith",
        "events": [
            {
                "event_type": "login",
                "timestamp": 1672579207
            },
            {
                "event_type": "purchase",
                "timestamp": 1672579208
            }
        ],
        "created_at": 1672579209,
        "updated_at": 1672579210
    },
    "tags": [
        {"text": "new_user", "created_at": 1672579211},
        {"text": "premium_member", "created_at": 1672579212}
    ],
    "created_at": 1672579209,
    "updated_at": 1672579210
}


def extract_field_values(record: Dict, fields_to_extract: Set = None):
    """Utility function to extract values from specified fields, including nested ones.

    Args:
        record (Dict): Record from which values to be extracted
        fields_to_extract (Set, optional): Set of field names to extract. Defaults to set().

    Returns:
        List: List of extracted field values
    """

    if fields_to_extract is None:
        fields_to_extract = set()

    extracted_field_values = []
    for key, value in record.items():
        if key in fields_to_extract:
            extracted_field_values.append(value)
        elif isinstance(value, dict):
            # Recurse on nested dictionary fields
            nested_fields = extract_field_values(value, fields_to_extract)
            extracted_field_values.extend(nested_fields)
        elif isinstance(value, list):
            # Loop on the List values and then recurse if item is a dict
            for item in value:
                if isinstance(item, dict):
                    nested_fields = extract_field_values(item, fields_to_extract)
                    extracted_field_values.extend(nested_fields)

    return extracted_field_values


class ConcreteParentBaseStream(IncrementalStream):
    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_keys(self):
        return ["updated_at"]

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def tap_stream_id(self):
        return "stream_1"

    def modify_object(self, record, parent_record=None, datetime_fields=set()):
        return super().modify_object(record, parent_record, datetime_fields)


class TestSync(unittest.TestCase):
    @patch("tap_delighted.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):

        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        self.stream = ConcreteParentBaseStream(catalog=mock_catalog)
        self.stream.client = MagicMock()
        self.stream.child_to_sync = []

    @patch("tap_delighted.streams.abstracts.get_bookmark", return_value=100)
    def test_write_bookmark_with_state(self, mock_get_bookmark):

        state = {'bookmarks': {'stream_1': {'updated_at': 100}}}
        result = self.stream.write_bookmark(state, "stream_1", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'stream_1': {'updated_at': 200}}})

    @patch("tap_delighted.streams.abstracts.get_bookmark", return_value=100)
    def test_write_bookmark_without_state(self, mock_get_bookmark):

        state = {}
        result = self.stream.write_bookmark(state, "stream_1", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'stream_1': {'updated_at': 200}}})

    @patch("tap_delighted.streams.abstracts.get_bookmark", return_value=300)
    def test_write_bookmark_with_old_value(self, mock_get_bookmark):

        state = {'bookmarks': {'stream_1': {'updated_at': 300}}}
        result = self.stream.write_bookmark(state, "stream_1", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'stream_1': {'updated_at': 300}}})

    @parameterized.expand([
        ["no datetime fields", RECORD_WITH_TIMESTAMP, set()],
        ["standard record", RECORD_WITH_TIMESTAMP, DATETIME_FIELDS],
        ["nested dict record", NESTED_DICT_RECORD_WITH_TIMESTAMP, DATETIME_FIELDS],
        ["nested array record", NESTED_ARRAY_RECORD_WITH_TIMESTAMP, DATETIME_FIELDS],
    ])
    def test_modify_object(self, test_name, dummy_record, datetime_fields):
        """ Test the modify_object method for converting timestamp fields to datetime strings.

        Args:
            test_name (str): Name of the test case
            dummy_record (Dict): The record to modify
            datetime_fields (Set): Set of fields to treat as datetime
        """
        # Extract timestamp values from the record before modification
        extracted_timestamp_values = extract_field_values(dummy_record, datetime_fields)

        # Convert extracted timestamp values to datetime strings for comparison
        expected_datetime_values = [get_datetime_from_timestamp(ts) for ts in extracted_timestamp_values]

        # Call the modify object method to parse the record and transform the datetime fields
        self.stream.modify_object(dummy_record, datetime_fields=datetime_fields)

        # Extract datetime values from the modified record
        actual_datetime_values = extract_field_values(dummy_record, datetime_fields)

        self.assertEqual(actual_datetime_values, expected_datetime_values)
