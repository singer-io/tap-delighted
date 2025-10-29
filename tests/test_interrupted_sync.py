
from base import delightedBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class delightedInterruptedSyncTest(InterruptedSyncTest, delightedBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_delighted_interrupted_sync_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def manipulate_state(self):
        return {
            "currently_syncing": "prospects",
            "bookmarks": {
                "people": {"created_at": "2020-01-01T00:00:00Z"},
                "survey_responses": {"updated_at": "2020-01-01T00:00:00Z"},
                "unsubscribes": {"unsubscribed_at": "2020-01-01T00:00:00Z"},
                "bounces": {"bounced_at": "2020-01-01T00:00:00Z"},
                "email_autopilot": {"updated_at": "2020-01-01T00:00:00Z"},
                "sms_autopilot": {"updated_at": "2020-01-01T00:00:00Z"},
            }
        }
