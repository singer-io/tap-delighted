import unittest
from unittest.mock import MagicMock, patch

from tap_delighted.discover import discover, is_stream_available
from tap_delighted.exceptions import (
    DelightedForbiddenError,
    DelightedUnauthorizedError,
    DelightedUnprocessableEntityError,
)


class TestDiscover(unittest.TestCase):
    test_stream_name = "test"

    dummy_schema = {
        test_stream_name: {
            "type": "object",
            "properties": {
                "id": {
                    "type": [
                        "null",
                        "string"
                    ]
                },
                "name": {
                    "type": [
                        "null",
                        "string"
                    ]
                },
                "email": {
                    "type": [
                        "null",
                        "string"
                    ]
                }
            }
        }
    }

    dummy_metadata = {
        test_stream_name: {
            (): {
                "breadcrumb": (),
                "table-key-properties": ["id"],
                "forced-replication-method": "FULL_TABLE",
                "valid-replication-keys": [],
            }
        }
    }

    @patch("tap_delighted.discover.get_schemas")
    @patch("singer.metadata.to_map")
    def test_discover(self, mock_to_map, mock_get_schemas):
        """ Test the discover function """

        mock_get_schemas.return_value = (self.dummy_schema, self.dummy_metadata)
        mock_to_map.return_value = self.dummy_metadata[self.test_stream_name]

        catalog_obj = discover()

        self.assertIsNotNone(catalog_obj)

        self.assertEqual(len(catalog_obj.streams), 1)
        self.assertEqual(catalog_obj.streams[0].stream, self.test_stream_name)

    def test_discovery_error(self):
        """ Test the discover function error handling """

        with patch("tap_delighted.discover.get_schemas") as mock_get_schemas:
            mock_get_schemas.return_value = ({"invalid_stream": "invalid_schema"}, {})

            with self.assertRaises(Exception):
                discover()

    @patch("tap_delighted.discover.get_schemas")
    @patch("singer.metadata.to_map")
    def test_discover_without_client(self, mock_to_map, mock_get_schemas):
        """Test that discover without client includes all streams (backward compatible)."""
        mock_get_schemas.return_value = (self.dummy_schema, self.dummy_metadata)
        mock_to_map.return_value = self.dummy_metadata[self.test_stream_name]

        catalog_obj = discover(client=None)

        self.assertEqual(len(catalog_obj.streams), 1)
        self.assertEqual(catalog_obj.streams[0].stream, self.test_stream_name)

    @patch("tap_delighted.discover.is_stream_available")
    @patch("tap_delighted.discover.get_schemas")
    @patch("singer.metadata.to_map")
    def test_discover_excludes_unavailable_streams(self, mock_to_map, mock_get_schemas, mock_available):
        """Test that discover excludes streams returning 422."""
        schemas = {
            "available_stream": self.dummy_schema[self.test_stream_name],
            "unavailable_stream": self.dummy_schema[self.test_stream_name],
        }
        field_metadata = {
            "available_stream": self.dummy_metadata[self.test_stream_name],
            "unavailable_stream": self.dummy_metadata[self.test_stream_name],
        }
        mock_get_schemas.return_value = (schemas, field_metadata)
        mock_to_map.return_value = self.dummy_metadata[self.test_stream_name]
        mock_available.side_effect = lambda client, name: name != "unavailable_stream"

        client = MagicMock()
        catalog_obj = discover(client=client)

        self.assertEqual(len(catalog_obj.streams), 1)
        self.assertEqual(catalog_obj.streams[0].stream, "available_stream")


class TestIsStreamAvailable(unittest.TestCase):

    @patch("tap_delighted.discover.STREAMS", {"my_stream": type("S", (), {"path": "v1/test.json"})})
    def test_available_stream(self):
        """Test that a stream returning 200 is available."""
        client = MagicMock()
        client.base_url = "https://api.delighted.com"
        client.make_request.return_value = {}

        self.assertTrue(is_stream_available(client, "my_stream"))

    @patch("tap_delighted.discover.STREAMS", {"my_stream": type("S", (), {"path": "v1/test.json"})})
    def test_unavailable_stream_422(self):
        """Test that a stream returning 422 is excluded."""
        client = MagicMock()
        client.base_url = "https://api.delighted.com"
        client.make_request.side_effect = DelightedUnprocessableEntityError("Autopilot not configured")

        self.assertFalse(is_stream_available(client, "my_stream"))

    @patch("tap_delighted.discover.STREAMS", {"my_stream": type("S", (), {"path": "v1/test.json"})})
    def test_unavailable_stream_401(self):
        """Test that a stream returning 401 is excluded."""
        client = MagicMock()
        client.base_url = "https://api.delighted.com"
        client.make_request.side_effect = DelightedUnauthorizedError("Unauthorized")

        self.assertFalse(is_stream_available(client, "my_stream"))

    @patch("tap_delighted.discover.STREAMS", {"my_stream": type("S", (), {"path": "v1/test.json"})})
    def test_unavailable_stream_403(self):
        """Test that a stream returning 403 is excluded."""
        client = MagicMock()
        client.base_url = "https://api.delighted.com"
        client.make_request.side_effect = DelightedForbiddenError("Forbidden")

        self.assertFalse(is_stream_available(client, "my_stream"))

    def test_unknown_stream_is_available(self):
        """Test that a stream not in STREAMS is treated as available."""
        client = MagicMock()
        self.assertTrue(is_stream_available(client, "nonexistent_stream"))
