from base import DelightedBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


class DelightedPaginationTest(PaginationTest, DelightedBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_delighted_pagination_test"

    def streams_to_test(self):
        streams_to_exclude = {"sms_autopilot", "metrics"}  # Excluding sms_autopilot since we don't have access to it, metrics -- Full Table
        return self.expected_stream_names().difference(streams_to_exclude)
