import json
import sys
import singer
import yaml

from singer import metadata
from tap_s3_csv.discover import discover_streams
from tap_s3_csv import s3
from tap_s3_csv.sync import sync_stream
from tap_s3_csv.config import CONFIG_CONTRACT, MUNGE_CONTRACT

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "search_prefix", "search_pattern", "table_name", "key_properties", "munge_config_file"]


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')
    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)
        table_spec = {
            "table_name": config["table_name"],
            "key_properties": config["key_properties"],
            "search_pattern": config["search_pattern"]
        }
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, table_spec, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


def load_munge_config(config):
    munge_config_file = config.get("munge_config_file", None)
    if munge_config_file:
        with open(munge_config_file) as fd:
            munge_config = yaml.safe_load(fd)
            return MUNGE_CONTRACT(munge_config)
    else:
        LOGGER.warning("No munge config file argument found")
        return None

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    LOGGER.info(config["key_properties"])
    config["key_properties"] = json.loads(config["key_properties"])
    LOGGER.info(config["key_properties"])


    config['munge'] = load_munge_config(config)

    try:
        for page in s3.list_files_in_bucket(config['bucket']):
            break
        LOGGER.warning("I have direct access to the bucket without assuming the configured role.")
    except:
        s3.setup_aws_client(config)

    if args.discover:
        do_discover(args.config)
    elif args.catalog:
        do_sync(config, args.catalog, args.state)


if __name__ == '__main__':
    main()
