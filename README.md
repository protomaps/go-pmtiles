# go-pmtiles

The single-file utility for creating and working with [PMTiles](https://github.com/protomaps/PMTiles) archives.

## Installation

See [Releases](https://github.com/protomaps/go-pmtiles/releases) for your OS and architecture.

## Creating a PMTiles archive from MBTiles

Convert an [MBTiles](https://github.com/mapbox/mbtiles-spec/tree/master/1.3) archive:

    pmtiles convert INPUT.mbtiles OUTPUT.pmtiles
    
## Create a PMTiles archive from a larger one

    pmtiles extract INPUT.pmtiles OUTPUT.pmtiles --region=REGION.geojson
    pmtiles extract https://example.com/INPUT.pmtiles OUTPUT.pmtiles --region=REGION.geojson
    pmtiles extract INPUT.pmtiles OUTPUT.pmtiles --maxzoom=MAXZOOM --bucket=s3://BUCKET_NAME
    
* `--region` a GeoJSON Polygon, Multipolygon, Feature, or FeatureCollection
* `--maxzoom=13`, `--minzoom=12` extract only a subset of zoom levels, see [docs for details](https://docs.protomaps.com/pmtiles/cli#extract)
* `--download-threads` parallel requests to speed up downloads
* `--overfetch` extra data to download to batch small requests: 0.05 is 5%

## Uploading
    
Upload an archive to S3-compatible cloud storage:

    # requires environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials
    pmtiles upload INPUT.pmtiles REMOTE.pmtiles --bucket=s3://BUCKET_NAME

## Inspecting archives

    pmtiles show INPUT.pmtiles
    pmtiles show INPUT.pmtiles --bucket=s3://BUCKET_NAME

## Serving Z/X/Y tiles

This section covers running a Z/X/Y tile server proxy for clients that read only those URLs. The simplest way to consume PMTiles on the web is directly in the browser: see the [JavaScript examples](https://github.com/protomaps/PMTiles/tree/main/js).
    
Serve a directory of archives from local or cloud storage as a Z/X/Y endpoint:

    pmtiles serve .
    # serves at http://localhost:8077/FILENAME/{z}/{x}/{y}.mvt

    pmtiles serve / --bucket=s3://BUCKET_NAME
    pmtiles serve prefix --bucket=s3://BUCKET_NAME
    
For production usage, it's recommended to run behind a reverse proxy like Nginx or Caddy to manage HTTP headers (`Access-Control-Allow-Origin`, `Cache-Control`...) and SSL certificates.

    pmtiles serve [FLAGS] BUCKET

* `--cors=ORIGIN` set the value of the Access-Control-Allow-Origin. * is a valid value but must be escaped in your shell. Appropriate for development use.
* `--cache-size=SIZE_MB` set the total size of the header and directory LRU cache. Default is 64 MB.
* `--port=PORT` specify the HTTP port.

Metadata is served at the URL path `/<archive_name>/metadata`.

Tiles are served at the URL path `/<archive_name>/<z>/<x>/<y>.<ext>`, where the extension `<ext>` is one of `mvt`, `png`, `jpg`, `webp`, `avif`.

## Remote URLs

Cloud storage URLs can be any URL recognized by [gocloud](https://gocloud.dev/concepts/urls/). Configure a custom endpoint and region:

```sh
s3://BUCKET_NAME?endpoint=https://example.com&region=REGION
```

You may need to escape special characters like `&` and `?` in your shell.

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
    
## Azure Specific Dev Notes
Run a local blobstore emulator:
```shell
podman run -p 10000:10000 -v .:/log  mcr.microsoft.com/azure-storage/azurite \ 
    azurite-blob --debug /log/debug.log 
```
Azurite accepts the same well-known account and key used by the legacy Azure Storage Emulator.

- Account name: `devstoreaccount1`
- Account key: `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`

Uploading files:
```sh
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
az storage  container create --name data       
az storage blob upload --file a1.pmtiles --container-name data --name a.pmtiles    
az storage blob upload --overwrite  --file a2.pmtiles --container-name data --name a.pmtiles    
```


Starting pmtiles:
```sh
AZURE_STORAGE_ACCOUNT=devstoreaccount1 AZURE_STORAGE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" ./go-pmtiles serve --port=8084 / --bucket="azblob://data?protocol=http&domain=127.0.0.1:10000"
```
