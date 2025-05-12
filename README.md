# go-pmtiles

The single-file utility for creating and working with [PMTiles](https://github.com/protomaps/PMTiles) archives.

## Installation

See [Releases](https://github.com/protomaps/go-pmtiles/releases) for your OS and architecture.

## Docs

See [docs.protomaps.com/pmtiles/cli](https://docs.protomaps.com/pmtiles/cli) for usage.

See [Go package docs](https://pkg.go.dev/github.com/protomaps/go-pmtiles/pmtiles) for API usage.

## Development

Run the program in development:

```sh
go run main.go
```

Run the test suite:

```sh
go test ./pmtiles
```

## Set datadog variables on Dockerfile:
In case DD_TRACE_ENABLED="true" ->

- Pattern to configure a custom filter span by resource_mame, exemple:
```bash
DD_RESOURCE_PATTERN='^(/hillshade_[^/]+)'
```

- Configure datadog sampling rules of each resource, exemple:
```bash
DD_TRACE_SAMPLING_RULES='[ \
  {"resource": "GET /hillshade*", "sample_rate": 1.0}, \
  {"resource": "GET /", "sample_rate": 0.0}, \
  {"resource": "HEAD /", "sample_rate": 0.0} \
]'
```
