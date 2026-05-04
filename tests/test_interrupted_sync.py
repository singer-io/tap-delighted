
from base import DelightedBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class DelightedInterruptedSyncTest(InterruptedSyncTest, DelightedBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_delighted_interrupted_sync_test"

    def streams_to_test(self):
        streams_to_exclude = {"metrics"}  # FullTable stream

        return self.expected_stream_names().difference(streams_to_exclude)

    def manipulate_state(self):
        return {
            "currently_syncing": "people",
            "bookmarks": {
                "people": {"created_at": "2020-01-01T00:00:00.000000Z"},
                "survey_responses": {"updated_at": "2020-01-01T00:00:00.000000Z"},
                "unsubscribes": {"unsubscribed_at": "2020-01-01T00:00:00.000000Z"},
                "bounces": {"bounced_at": "2020-01-01T00:00:00.000000Z"},
                "email_autopilot": {"updated_at": "2020-01-01T00:00:00.000000Z"},
            }
        }
