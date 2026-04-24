from base import DelightedBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class DelightedAllFields(AllFieldsTest, DelightedBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    MISSING_FIELDS = {
        "people": [
            "phone_number"  # As we couldn't generate this data
        ]
    }

    @staticmethod
    def name():
        return "tap_tester_delighted_all_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
