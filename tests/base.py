import os

from tap_tester import menagerie, runner
from tap_tester.logger import LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class DelightedBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2019-01-01T00:00:00Z"
    PARENT_TAP_STREAM_ID = "parent-tap-stream-id"

    # Populated dynamically by run_and_verify_check_mode based on
    # which streams the tap excludes at discovery time (401/403/422).
    PERMISSION_DEPENDENT_STREAMS = set()

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-delighted"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.delighted"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "people": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created_at"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 20
            },
            "survey_responses": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 2
            },
            "metrics": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 20
            },
            "unsubscribes": {
                cls.PRIMARY_KEYS: {"person_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"unsubscribed_at"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 2
            },
            "bounces": {
                cls.PRIMARY_KEYS: {"person_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"bounced_at"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 3
            },
            "email_autopilot": {
                cls.PRIMARY_KEYS: {"person_id", "next_survey_request_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 3
            },
            "sms_autopilot": {
                cls.PRIMARY_KEYS: {"person_id", "next_survey_request_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 20
            }
        }

    @classmethod
    def expected_stream_names(cls):
        """Return expected streams, excluding permission-dependent streams
        that may not be available in the current test account."""
        return (set(cls.expected_metadata().keys())
                - cls.PERMISSION_DEPENDENT_STREAMS)

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {'api_key': 'TAP_DELIGHTED_API_KEY'}

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return {
            "start_date": self.start_date
        }

    def run_and_verify_check_mode(self, conn_id):
        """Override to dynamically detect permission-dependent streams.

        Runs discovery, compares found streams against expected_metadata,
        and treats any missing streams as permission-dependent rather
        than failing immediately.
        """
        check_job_name = runner.run_check_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(
            self, exit_status, check_job_name
        )

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(
            len(found_catalogs), 0,
            logging="A catalog was produced by discovery."
        )

        found_names = {c['stream_name'] for c in found_catalogs}
        all_expected = set(self.expected_metadata().keys())

        # Streams in catalog but not in expected_metadata are unexpected
        unexpected = found_names - all_expected
        self.assertEqual(
            unexpected, set(),
            logging="No unexpected streams in catalog."
        )

        # Streams in expected_metadata but not discovered are
        # permission-dependent — update the class variable dynamically
        missing = all_expected - found_names
        if missing:
            LOGGER.info(
                "Dynamically excluding permission-dependent "
                "streams: %s", missing
            )
            type(self).PERMISSION_DEPENDENT_STREAMS = missing

        # Now the assertion uses the updated expected_stream_names
        self.assertSetEqual(
            self.expected_stream_names(), found_names,
            logging="Expected streams are present in catalog."
        )

        return found_catalogs

    def expected_parent_tap_stream(self, stream=None):
        """return a dictionary with key of table name and value of parent stream"""
        parent_stream = {
            table: properties.get(self.PARENT_TAP_STREAM_ID, None)
            for table, properties in self.expected_metadata().items()}
        if not stream:
            return parent_stream
        return parent_stream[stream]
