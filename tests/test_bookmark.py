from base import DelightedBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class DelightedBookMarkTest(BookmarkTest, DelightedBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "people": {"created_at": "2020-01-01T00:00:00.000000Z"},
            "survey_responses": {"updated_at": "2020-01-01T00:00:00.000000Z"},
            "unsubscribes": {"unsubscribed_at": "2020-01-01T00:00:00.000000Z"},
            "bounces": {"bounced_at": "2020-01-01T00:00:00.000000Z"},
            "email_autopilot": {"updated_at": "2020-01-01T00:00:00.000000Z"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_delighted_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {"sms_autopilot",  # We don't have API access to it
                              "metrics"}        # FullTable stream

        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""
        new_bookmarks = {
            "people": {"created_at": "2025-11-01T00:00:00.000000Z"},
            "survey_responses": {"updated_at": "2025-11-05T00:00:00.000000Z"},
            "unsubscribes": {"unsubscribed_at": "2025-11-01T00:00:00.000000Z"},
            "bounces": {"bounced_at": "2025-11-01T00:00:00.000000Z"},
            "email_autopilot": {"updated_at": "2025-11-04T00:00:00.000000Z"},
        }

        return new_bookmarks
