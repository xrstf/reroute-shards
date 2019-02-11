# reroute-shards

This program communicates with Elasticsearch's REST API and reroutes unassigned shards
to the least loaded nodes.

## Usage

    $ make
    $ ./reroute-shards [endpoint=localhost:9200]

There is no support for TLS endpoints. Tested with Elasticsearch 6.5.

## License

WTFPL
