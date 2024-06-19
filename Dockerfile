FROM golang:1.21.11-alpine3.19 AS builder
COPY . /workspace
WORKDIR /workspace
ENV CGO_ENABLED=0
RUN go build -o /workspace/go-pmtiles
FROM gcr.io/distroless/static
COPY --from=builder /workspace/go-pmtiles /go-pmtiles
EXPOSE 8080
ENTRYPOINT ["/go-pmtiles"]
