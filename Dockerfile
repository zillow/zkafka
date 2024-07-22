FROM golang:1.22 AS build

ENV CGO_ENABLED=1
ENV GOPROXY=https://proxy.golang.org\|https://artifactory.zgtools.net/artifactory/api/go/devex-go\|direct
ENV GONOSUMDB=*gitlab.zgtools.net*

WORKDIR /go/src/zkafka
COPY . .

RUN go mod download
RUN go build -o zkafka

FROM debian
COPY --from=build /go/src/zkafka /
ENTRYPOINT ["/zkafka"]

