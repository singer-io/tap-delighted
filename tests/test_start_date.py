from base import DelightedBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class DelightedStartDateTest(StartDateTest, DelightedBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_delighted_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {"sms_autopilot", "metrics"}  # Excluding sms_autopilot since we don't have access to it and full_table metrics
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2015-03-25T00:00:00Z"

    @property
    def start_date_2(self):
        return "2025-11-05T00:00:00Z"
