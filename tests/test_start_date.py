from base import DelightedBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class CachedStartDateMixin:
    """Mixin that isolates the shared StartDateTest cache per concrete class.

    StartDateTest stores sync results as class-level attributes, so all
    subclasses share the same slots.  This mixin keeps a per-class snapshot of
    those attributes (using ``type(self)`` so the logic is rename-safe) and
    restores/captures them around each ``setUp`` call so that every concrete
    test class sees only its own cached data.
    """

    # Per-class sync cache – initialised to None in each concrete class.
    _cached_record_count_1 = None
    _cached_messages_1 = None
    _cached_record_count_2 = None
    _cached_messages_2 = None

    def setUp(self):
        cls = type(self)
        # Restore this class's snapshot into the shared StartDateTest slots
        # so that setUp's early-exit condition evaluates correctly.
        StartDateTest.record_count_by_stream_1 = cls._cached_record_count_1
        StartDateTest.synced_messages_by_stream_1 = cls._cached_messages_1
        StartDateTest.record_count_by_stream_2 = cls._cached_record_count_2
        StartDateTest.synced_messages_by_stream_2 = cls._cached_messages_2

        super().setUp()

        # Capture whatever setUp wrote back into the shared slots.
        cls._cached_record_count_1 = StartDateTest.record_count_by_stream_1
        cls._cached_messages_1 = StartDateTest.synced_messages_by_stream_1
        cls._cached_record_count_2 = StartDateTest.record_count_by_stream_2
        cls._cached_messages_2 = StartDateTest.synced_messages_by_stream_2


class DelightedStartDateTest(CachedStartDateMixin, StartDateTest, DelightedBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    _cached_record_count_1 = None
    _cached_messages_1 = None
    _cached_record_count_2 = None
    _cached_messages_2 = None

    @staticmethod
    def name():
        return "tap_tester_delighted_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {
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


class DelightedEmailAutopilotStartDateTest(CachedStartDateMixin, StartDateTest, DelightedBaseTest):
    """Start date test specifically for email_autopilot.

    The email_autopilot stream has records with updated_at ranging from
    2026-01-27 to 2026-04-24. Use start_date_1 (2025-12-01) to capture all
    records, and start_date_2 (2026-03-15) to capture only a subset.
    """

    _cached_record_count_1 = None
    _cached_messages_1 = None
    _cached_record_count_2 = None
    _cached_messages_2 = None

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
        return "2026-03-15T00:00:00.000000Z"
