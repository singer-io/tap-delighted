import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import singer

LOGGER = singer.get_logger()

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BOOKMARK_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def get_datetime_from_timestamp(timestamp: Optional[int]) -> Optional[str]:
    """Convert a timestamp to an ISO 8601 formatted date-time string.
    If the timestamp is None, return None.

    Args:
        timestamp (Optional[int]): The timestamp in seconds.

    Returns:
        Optional[str]: The ISO 8601 formatted date-time string.
    """

    if not timestamp:
        return None

    datetime_obj = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return datetime_obj.strftime(DATETIME_FORMAT)


def get_timestamp_from_datetime(date_str: Optional[str]) -> Optional[int]:
    """Convert an ISO 8601 formatted date-time string to a timestamp in seconds.
    If the date_str is None, return None.

    Args:
        date_str (Optional[str]): The ISO 8601 formatted date-time string.

    Returns:
        Optional[int]: The timestamp in seconds.
    """

    if date_str is None:
        return None

    try:
        dt = datetime.strptime(date_str, DATETIME_FORMAT).replace(tzinfo=timezone.utc)
    except Exception:
        dt = datetime.strptime(date_str, BOOKMARK_FORMAT).replace(tzinfo=timezone.utc)

    return int(dt.timestamp())


def get_datetime_fields_from_schema(schema: Dict) -> Set[str]:
    """ Function to get datetime fields from the schema

    Args:
        schema (Dict): Schema specific to a stream

    Returns:
        Set[str]: Set of fields with format date-time
    """

    schema_properties = schema.get("properties", {})
    datetime_fields = set()

    for field, field_props in schema_properties.items():
        # First check the type of the field
        if (
            field_props.get("type") == ["null", "string"] and
            field_props.get("format") == "date-time"
        ):
            # This is a date-time field, so add in the list
            datetime_fields.add(field)

        elif field_props.get("type") == ["null", "object"]:
            # This can have nested date-time fields
            nested_props = {"properties": field_props.get("properties", {})}
            datetime_nested_fields = get_datetime_fields_from_schema(schema=nested_props)
            datetime_fields.update(datetime_nested_fields)

        elif field_props.get("type") == ["null", "array"]:
            # This can have nested date-time fields in items
            items_props = {"properties": field_props.get("items", {}).get("properties", {})}
            datetime_nested_fields = get_datetime_fields_from_schema(schema=items_props)
            datetime_fields.update(datetime_nested_fields)

    return datetime_fields


def normalize_autopilot_record(record: Dict, key_properties: List):
    """ Normalise the autopilot stream records by moving key_properties at root level

    Args:
        record (Dict): Autopilot stream record
        key_properties (List): List of key properties for the stream
    """

    person_id = record.get("person", {}).get("id")
    next_survey_request_id = record.get("next_survey_request", {}).get("id")

    for key_field in key_properties:
        if key_field == "person_id" and person_id:
            record["person_id"] = person_id
        if key_field == "next_survey_request_id" and next_survey_request_id:
            record["next_survey_request_id"] = next_survey_request_id


class DelightedPaginator:
    """ Class to paginate Delighted API responses """

    def __init__(self, client, params: Dict = {},
                 headers: Dict = {}, http_method: str = "GET",
                 path: str = "", url_endpoint: str = "") -> None:
        self._client = client
        self._params = params
        self._headers = headers
        self._http_method = http_method
        self._path = path
        self._url_endpoint = url_endpoint
        self._data_payload = {}
        self._page = 1

    @property
    def _get_params(self):
        return self._params

    @property
    def _get_headers(self):
        return self._headers

    def _make_request(self, **kwargs) -> Any:
        response = self._client.make_request(
                self._http_method,
                self._url_endpoint,
                self._params,
                self._headers,
                body=json.dumps(self._data_payload),
                path=self._path,
                **kwargs
            )

        return response

    def _page_number_pagination(self):
        """ Generator to paginate using page number

        Yields:
            Dict: A record from the API response
        """

        while True:
            self._params["page"] = self._page
            LOGGER.info("Fetching page number: {} for endpoint {}".format(self._page, self._path))

            response = self._make_request()
            if not response:
                break

            if isinstance(response, dict):
                raw_records = response.get("data", [response])
            else:
                raw_records = response

            LOGGER.info("Fetched {} records for stream".format(len(raw_records)))

            if not raw_records:
                break

            yield from raw_records
            self._page += 1

    def _cursor_pagination(self):
        """ Generator to paginate using cursor from Link header

        Yields:
            Dict: A record from the API response
        """

        next_url = self._url_endpoint

        while next_url:
            LOGGER.info("Fetching URL: {}".format(next_url))
            response = self._make_request(full_response=True)
            raw_records = response.json()

            if isinstance(raw_records, dict):
                raw_records = raw_records.get("data", [raw_records])

            LOGGER.info("Fetched {} records".format(len(raw_records)))

            yield from raw_records

            link_header = response.headers.get("Link")
            if link_header and 'rel="next"' in link_header:
                # regex match to group the link header
                match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
                next_url = match.group(1) if match else None
                self._url_endpoint = next_url
            else:
                break
