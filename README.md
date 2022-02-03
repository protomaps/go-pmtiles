# go-pmtiles

A caching proxy for the serverless [PMTiles](github.com/protomaps/pmtiles) archive format. Resolves several of the limitations of PMTiles by running a minimal single-binary process on a tiny instance:

* backwards compatibility for map renderers that require {z}/{x}/{y} url endpoints
* lower latency, multiple fetches for index retrieval not necessary
* automatic gzip compression for vector tiles

For production usage, it's recommended to run go-pmtiles behind a reverse proxy like Nginx or Caddy to manage HTTP headers (`Access-Control-Allow-Origin`, `Cache-Control`...) and SSL certificates. 

## Installation

See [Releases](https://github.com/protomaps/go-pmtiles/releases) for your OS and architecture.

## Usage

To serve a local directory containing EXAMPLE.pmtiles:

    go-pmtiles serve EXAMPLE_DIR
    > Serving EXAMPLE_DIR on HTTP port: 8077

Will respond to requests at http://localhost:8077/EXAMPLE/{z}/{x}/{y}.pbf

To serve a S3 bucket containing EXAMPLE.pmtiles:

    go-pmtiles serve https://EXAMPLE_BUCKET.s3-us-west-2.amazonaws.com
    > Serving https://EXAMPLE_BUCKET.s3-us-west-2.amazonaws.com on HTTP port: 8077
    
Will respond to requests at http://localhost:8077/EXAMPLE/{z}/{x}/{y}.pbf    
  
Option flags must come before EXAMPLE_DIR or EXAMPLE_BUCKET.

* `-cors ORIGIN` set the value of the Access-Control-Allow-Origin. * is a valid value but must be escaped in bash. Appropriate for development use.
* `-cache SIZE_MB` set the total size of the successful response cache. Default is 64 MB. Cache entries are evicted in LRU order.

To create a new PMTiles archive from a subset of another:

    go-pmtiles subpyramid INPUT.pmtiles OUTPUT.pmtiles ZOOM MIN_X MIN_Y MAX_X MAX_Y
    

