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
        streams_to_exclude = {"metrics"}  # FullTable stream

        return self.expected_stream_names().difference(streams_to_exclude)
