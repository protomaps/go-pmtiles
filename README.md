# go-pmtiles

The single-file utility for creating and working with [PMTiles](github.com/protomaps/PMTiles) archives. 

## Installation

See [Releases](https://github.com/protomaps/go-pmtiles/releases) for your OS and architecture.

## Usage

Convert an [MBTiles](https://github.com/mapbox/mbtiles-spec/tree/master/1.3) archive:

    pmtiles convert INPUT.mbtiles OUTPUT.pmtiles
    
Upgrade a PMTiles version 2 to the latest version:

    pmtiles convert INPUT.pmtiles OUTPUT.pmtiles
    
Upload an archive to S3-compatible cloud storage:

    # requires environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials
    pmtiles upload ARCHIVE.pmtiles s3://BUCKET_NAME?endpoint=https://example.com&region=region
    
Serve a directory of archive from local or cloud storage as a universally compatible Z/X/Y URL: *Note: not working yet for v3*

    # EXAMPLE_DIR contains FILENAME.pmtiles
    pmtiles serve EXAMPLE_DIR 
    # serves at http://localhost:8077/FILENAME/{z}/{x}/{y}.pbf
    pmtiles serve s3://MY_BUCKET
    
*For production usage, it's recommended to run behind a reverse proxy like Nginx or Caddy to manage HTTP headers (`Access-Control-Allow-Origin`, `Cache-Control`...) and SSL certificates. *

Option flags must come before EXAMPLE_DIR or EXAMPLE_BUCKET.

* `-cors ORIGIN` set the value of the Access-Control-Allow-Origin. * is a valid value but must be escaped in bash. Appropriate for development use.
* `-cache SIZE_MB` set the total size of the successful response cache. Default is 64 MB. Cache entries are evicted in LRU order.
    

