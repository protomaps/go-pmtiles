# go-pmtiles

The single-file utility for creating and working with [PMTiles](https://github.com/protomaps/PMTiles) archives.

## Installation

See [Releases](https://github.com/protomaps/go-pmtiles/releases) for your OS and architecture.

MacOS users temporarily need a [few extra steps on running app for the first time.](https://github.com/hashicorp/terraform/issues/23033#issuecomment-542302933)

## Creating PMTiles archives

Convert an [MBTiles](https://github.com/mapbox/mbtiles-spec/tree/master/1.3) archive:

    pmtiles convert INPUT.mbtiles OUTPUT.pmtiles
    
Upgrade a PMTiles version 2 to the latest version:

    pmtiles convert INPUT.pmtiles OUTPUT.pmtiles

## Uploading
    
Upload an archive to S3-compatible cloud storage:

    # requires environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials
    pmtiles upload LOCAL.pmtiles s3://BUCKET_NAME?endpoint=https://example.com&region=region REMOTE.pmtiles

## Inspecting archives

    pmtiles show file:// example.pmtiles
    pmtiles show "s3://BUCKET_NAME?region=us-west-2" example.pmtiles

## Serving Z/X/Y tiles

This section covers running a Z/X/Y tile server proxy for clients that read only those URLs. The simplest way to consume PMTiles on the web is directly in the browser: see the [JavaScript examples](https://github.com/protomaps/PMTiles/tree/master/js).
    
Serve a directory of archive from local or cloud storage as a universally compatible Z/X/Y endpoint:

    pmtiles serve file://.
    # serves at http://localhost:8077/FILENAME/{z}/{x}/{y}.pbf

    pmtiles serve "s3://example_bucket?region=us-west-2"
    
For production usage, it's recommended to run behind a reverse proxy like Nginx or Caddy to manage HTTP headers (`Access-Control-Allow-Origin`, `Cache-Control`...) and SSL certificates.

    pmtiles serve [FLAGS] BUCKET

* `-cors=ORIGIN` set the value of the Access-Control-Allow-Origin. * is a valid value but must be escaped in bash. Appropriate for development use.
* `-cache=SIZE_MB` set the total size of the header and directory LRU cache. Default is 64 MB.
* `-port=PORT` specify the HTTP port.

## Cloud Storage Permissions

Example minimal S3 policy for writing/reading to a bucket:

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": "arn:aws:s3:::my-bucket-name/*"
            }
        ]
    }
