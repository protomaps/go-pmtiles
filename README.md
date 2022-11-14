# go-pmtiles

The single-file utility for creating and working with [PMTiles](https://github.com/protomaps/PMTiles) archives.

## Installation

See [Releases](https://github.com/protomaps/go-pmtiles/releases) for your OS and architecture.

## Creating PMTiles archives

Convert an [MBTiles](https://github.com/mapbox/mbtiles-spec/tree/master/1.3) archive:

    pmtiles convert INPUT.mbtiles OUTPUT.pmtiles
    
Upgrade a PMTiles version 2 to the latest version:

    pmtiles convert INPUT.pmtiles OUTPUT.pmtiles

## Uploading
    
Upload an archive to S3-compatible cloud storage:

    # requires environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials
    pmtiles upload INPUT.pmtiles s3://BUCKET_NAME REMOTE.pmtiles

## Inspecting archives

    pmtiles show file:// INPUT.pmtiles
    pmtiles show "s3://BUCKET_NAME" INPUT.pmtiles

## Serving Z/X/Y tiles

This section covers running a Z/X/Y tile server proxy for clients that read only those URLs. The simplest way to consume PMTiles on the web is directly in the browser: see the [JavaScript examples](https://github.com/protomaps/PMTiles/tree/master/js).
    
Serve a directory of archives from local or cloud storage as a Z/X/Y endpoint:

    pmtiles serve file://.
    # serves at http://localhost:8077/FILENAME/{z}/{x}/{y}.pbf

    pmtiles serve "s3://BUCKET_NAME?region=REGION"
    
For production usage, it's recommended to run behind a reverse proxy like Nginx or Caddy to manage HTTP headers (`Access-Control-Allow-Origin`, `Cache-Control`...) and SSL certificates.

    pmtiles serve [FLAGS] BUCKET

* `-cors=ORIGIN` set the value of the Access-Control-Allow-Origin. * is a valid value but must be escaped in bash. Appropriate for development use.
* `-cache=SIZE_MB` set the total size of the header and directory LRU cache. Default is 64 MB.
* `-port=PORT` specify the HTTP port.

## Remote URLs

Cloud storage URLs can be any URL recognized by [gocloud](https://gocloud.dev/concepts/urls/). Configure a custom endpoint and region:

```sh
s3://BUCKET_NAME?endpoint=https://example.com&region=REGION
```

## Cloud Storage Permissions

To upload your files to AWS S3 you will need an IAM policy for writing/reading to a bucket, at minimum this:

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
    
