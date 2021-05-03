# go-pmtiles

## Installation

    go install github.com/protomaps/go-pmtiles
    
## Usage

To serve a local directory containing EXAMPLE.pmtiles:

    go-pmtiles EXAMPLE_DIR
    > Serving EXAMPLE_DIR on HTTP port: 8077

Will respond to requests at http://localhost:8077/EXAMPLE/{z}/{x}/{y}.pbf

To serve a S3 bucket containing EXAMPLE.pmtiles:

    go-pmtiles https://EXAMPLE_BUCKET.s3-us-west-2.amazonaws.com
    > Serving https://EXAMPLE_BUCKET.s3-us-west-2.amazonaws.com on HTTP port: 8077
    
Will respond to requests at http://localhost:8077/EXAMPLE/{z}/{x}/{y}.pbf    
  
Option flags must come before EXAMPLE_DIR or EXAMPLE_BUCKET.

* `-cors ORIGIN` set the value of the Access-Control-Allow-Origin. * is a valid value but must be escaped in bash.
* `-cache SIZE_MB` set the total size of the successful response cache. Default is 64 MB. Cache entries are evicted in LRU order.
