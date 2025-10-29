from base import delightedBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class delightedBookMarkTest(BookmarkTest, delightedBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "people": {"created_at": "2020-01-01T00:00:00Z"},
            "survey_responses": {"updated_at": "2020-01-01T00:00:00Z"},
            "unsubscribes": {"unsubscribed_at": "2020-01-01T00:00:00Z"},
            "bounces": {"bounced_at": "2020-01-01T00:00:00Z"},
            "email_autopilot": {"updated_at": "2020-01-01T00:00:00Z"},
            "sms_autopilot": {"updated_at": "2020-01-01T00:00:00Z"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_delighted_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)
