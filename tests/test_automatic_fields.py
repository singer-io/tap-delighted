"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import DelightedBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class DelightedAutomaticFields(MinimumSelectionTest, DelightedBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_delighted_automatic_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {"sms_autopilot"}  # Excluding sms_autopilot since we don't have access to it
        return self.expected_stream_names().difference(streams_to_exclude)
