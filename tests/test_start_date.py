from base import DelightedBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class DelightedStartDateTest(StartDateTest, DelightedBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    # Per-class sync cache – isolates this class from other StartDateTest
    # subclasses that also store results on the shared StartDateTest attributes.
    _cached_record_count_1 = None
    _cached_messages_1 = None
    _cached_record_count_2 = None
    _cached_messages_2 = None

    def setUp(self):
        # Restore or clear the shared StartDateTest cache with this class's
        # own snapshot so setUp's condition works correctly.
        StartDateTest.record_count_by_stream_1 = DelightedStartDateTest._cached_record_count_1
        StartDateTest.synced_messages_by_stream_1 = DelightedStartDateTest._cached_messages_1
        StartDateTest.record_count_by_stream_2 = DelightedStartDateTest._cached_record_count_2
        StartDateTest.synced_messages_by_stream_2 = DelightedStartDateTest._cached_messages_2
        super().setUp()

        DelightedStartDateTest._cached_record_count_1 = StartDateTest.record_count_by_stream_1
        DelightedStartDateTest._cached_messages_1 = StartDateTest.synced_messages_by_stream_1
        DelightedStartDateTest._cached_record_count_2 = StartDateTest.record_count_by_stream_2
        DelightedStartDateTest._cached_messages_2 = StartDateTest.synced_messages_by_stream_2

    @staticmethod
    def name():
        return "tap_tester_delighted_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "sms_autopilot",  # We don't have API access to it
            "metrics",  # FullTable stream
            "email_autopilot",  # Tested separately with dates that produce differing record counts
        }

        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2015-03-25T00:00:00.000000Z"

    @property
    def start_date_2(self):
        return "2025-11-05T00:00:00.000000Z"


class DelightedEmailAutopilotStartDateTest(StartDateTest, DelightedBaseTest):
    """Start date test specifically for email_autopilot.

    The email_autopilot stream has records starting from 2025-12-09, so the
    general test's start_date_2 (2025-11-05) yields the same 28 records as
    start_date_1. Use dates where start_date_2 (2026-02-01) cuts the result
    to 13 records while start_date_1 (2025-12-01) returns all 28.
    """
    _cached_record_count_1 = None
    _cached_messages_1 = None
    _cached_record_count_2 = None
    _cached_messages_2 = None

    def setUp(self):
        StartDateTest.record_count_by_stream_1 = DelightedEmailAutopilotStartDateTest._cached_record_count_1
        StartDateTest.synced_messages_by_stream_1 = DelightedEmailAutopilotStartDateTest._cached_messages_1
        StartDateTest.record_count_by_stream_2 = DelightedEmailAutopilotStartDateTest._cached_record_count_2
        StartDateTest.synced_messages_by_stream_2 = DelightedEmailAutopilotStartDateTest._cached_messages_2
        super().setUp()

        DelightedEmailAutopilotStartDateTest._cached_record_count_1 = StartDateTest.record_count_by_stream_1
        DelightedEmailAutopilotStartDateTest._cached_messages_1 = StartDateTest.synced_messages_by_stream_1
        DelightedEmailAutopilotStartDateTest._cached_record_count_2 = StartDateTest.record_count_by_stream_2
        DelightedEmailAutopilotStartDateTest._cached_messages_2 = StartDateTest.synced_messages_by_stream_2

    @staticmethod
    def name():
        return "tap_tester_delighted_email_autopilot_start_date_test"

    def streams_to_test(self):
        return {"email_autopilot"}

    @property
    def start_date_1(self):
        return "2025-12-01T00:00:00.000000Z"

    @property
    def start_date_2(self):
        return "2026-02-01T00:00:00.000000Z"
