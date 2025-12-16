FROM golang:1.24-alpine AS builder
COPY . /workspace
WORKDIR /workspace
ENV CGO_ENABLED=0
ARG VERSION
RUN go build \
  -trimpath \
  -ldflags "\
    -s -w \
    -X main.version=${VERSION}" \
  -o /workspace/go-pmtiles
FROM gcr.io/distroless/static
COPY --from=builder /workspace/go-pmtiles /go-pmtiles
EXPOSE 8080
ENTRYPOINT ["/go-pmtiles"]
