package caddy

import (
	"fmt"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"go.uber.org/zap"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

func init() {
	caddy.RegisterModule(Middleware{})
	httpcaddyfile.RegisterHandlerDirective("pmtiles_proxy", parseCaddyfile)
}

// Middleware creates a Z/X/Y tileserver backed by a local or remote bucket of PMTiles archives.
type Middleware struct {
	Bucket    string `json:"bucket"`
	CacheSize int    `json:"cache_size"`
	PublicURL string `json:"public_url"`
	logger    *zap.Logger
	server    *pmtiles.Server
}

// CaddyModule returns the Caddy module information.
func (Middleware) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.pmtiles_proxy",
		New: func() caddy.Module { return new(Middleware) },
	}
}

func (m *Middleware) Provision(ctx caddy.Context) error {
	m.logger = ctx.Logger()
	logger := log.New(io.Discard, "", log.Ldate)
	prefix := "." // serve only the root of the bucket for now, at the root route of Caddyfile
	server, err := pmtiles.NewServer(m.Bucket, prefix, logger, m.CacheSize, "", m.PublicURL)
	if err != nil {
		return err
	}
	m.server = server
	server.Start()
	return nil
}

func (m *Middleware) Validate() error {
	if m.Bucket == "" {
		return fmt.Errorf("no bucket")
	}
	if m.CacheSize <= 0 {
		m.CacheSize = 64
	}
	return nil
}

func (m Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	start := time.Now()
	statusCode, headers, body := m.server.Get(r.Context(), r.URL.Path)
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(statusCode)
	w.Write(body)
	m.logger.Info("response", zap.Int("status", statusCode), zap.String("path", r.URL.Path), zap.Duration("duration", time.Since(start)))

	return next.ServeHTTP(w, r)
}

func (m *Middleware) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			switch d.Val() {
			case "bucket":
				if !d.Args(&m.Bucket) {
					return d.ArgErr()
				}
			case "cache_size":
				var cacheSize string
				if !d.Args(&cacheSize) {
					return d.ArgErr()
				}
				num, err := strconv.Atoi(cacheSize)
				if err != nil {
					return d.ArgErr()
				}
				m.CacheSize = num
			case "public_url":
				if !d.Args(&m.PublicURL) {
					return d.ArgErr()
				}
			}
		}
	}
	return nil
}

func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var m Middleware
	err := m.UnmarshalCaddyfile(h.Dispenser)
	return m, err
}

var (
	_ caddy.Provisioner           = (*Middleware)(nil)
	_ caddy.Validator             = (*Middleware)(nil)
	_ caddyhttp.MiddlewareHandler = (*Middleware)(nil)
	_ caddyfile.Unmarshaler       = (*Middleware)(nil)
)
