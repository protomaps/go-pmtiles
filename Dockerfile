FROM golang:1.16.5 AS builder
COPY . /workspace
WORKDIR /workspace
ENV CGO_ENABLED=0
RUN go build -o /workspace/go-pmtiles
FROM scratch
COPY --from=builder /workspace/go-pmtiles /workspace/go-pmtiles
EXPOSE 8080
ENTRYPOINT ["/workspace/go-pmtiles"]
