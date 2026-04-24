import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_delighted.exceptions import DelightedUnprocessableEntityError
from tap_delighted.schema import get_schemas
from tap_delighted.streams import STREAMS

LOGGER = singer.get_logger()


def is_stream_available(client, stream_name):
    """
    Probe a stream's API endpoint to check if it is available.
    Returns False if the API returns 422 (feature not configured).
    """
    stream_cls = STREAMS.get(stream_name)
    if not stream_cls:
        return True

    path = getattr(stream_cls, 'path', '')
    if not path:
        return True

    endpoint = f"{client.base_url}/{path}"
    params = {"per_page": 1}
    headers = {"Content-Type": "application/json"}

    try:
        client.make_request("GET", endpoint, params=params, headers=headers)
        return True
    except DelightedUnprocessableEntityError:
        LOGGER.warning(
            "Excluding stream '%s' from catalog: API returned 422. "
            "This stream requires a feature not configured in the Delighted account.",
            stream_name,
        )
        return False


def discover(client=None) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    """
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        if client and not is_stream_available(client, stream_name):
            continue

        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: {}".format(stream_name))
            LOGGER.error("type schema_dict: {}".format(type(schema_dict)))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
