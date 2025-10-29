"""Test tap discovery mode and metadata."""
from base import delightedBaseTest
from tap_tester import menagerie
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class delightedDiscoveryTest(DiscoveryTest, delightedBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""
    orphan_streams = {
        people,
        survey_responses,
        metrics,
        unsubscribes,
        bounces,
        email_autopilot,
        sms_autopilot,
    }

    @staticmethod
    def name():
        return "tap_tester_delighted_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def test_parent_stream(self):
        """
        Test that each stream's metadata correctly includes the expected parent tap stream ID.

        For each stream in `streams_to_test`, this test:
        - Retrieves the expected parent tap stream ID from test expectations.
        - Retrieves the actual metadata from the found catalog.
        - Verifies that the metadata contains the `PARENT_TAP_STREAM_ID` key (except for the orphans stream).
        - Confirms that the actual parent tap stream ID matches the expected value.
        """
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                expected_parent_tap_stream_id = self.expected_parent_tap_stream(stream)

                catalog = [catalog for catalog in self.found_catalogs
                           if catalog["stream_name"] == stream][0]
                metadata = menagerie.get_annotated_schema(
                    self.conn_id, catalog['stream_id'])["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_parent_tap_stream_id = \
                    stream_properties[0].get("metadata", {}).get(self.PARENT_TAP_STREAM_ID, None)

                self.assertIn("metadata", stream_properties[0])
                stream_metadata = stream_properties[0]["metadata"]

                if stream not in self.orphan_streams:
                    self.assertIn(self.PARENT_TAP_STREAM_ID, stream_metadata)
                    self.assertTrue(isinstance(actual_parent_tap_stream_id, str))

                with self.subTest(msg="validating parent tap stream id"):
                    self.assertEqual(expected_parent_tap_stream_id, actual_parent_tap_stream_id,
                                        logging=f"verify {expected_parent_tap_stream_id} "
                                                f"is saved in metadata as a parent-tap-stream-id")
