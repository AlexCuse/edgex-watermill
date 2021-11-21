FROM golang:1.16-alpine3.14 AS builder

# add git for go modules
RUN apk update && apk add --no-cache make git gcc libc-dev libsodium-dev zeromq-dev
WORKDIR /temp

#ARG GITHUB_KEY
#RUN go env -w GOPRIVATE=github.com/technotects
#RUN git config --global url."https://golang:$GITHUB_KEY@github.com".insteadOf "https://github.com"

COPY go.mod go.mod

RUN go mod download

COPY . .
COPY ./res ./res

RUN go build -o ./edgex-watermill-consumer ./...

RUN apk info -a zeromq-dev

# Next image - Copy built Go binary into new workspace
FROM alpine

RUN apk add --no-cache zeromq

RUN apk info -a zeromq

COPY --from=builder /temp/res /res
COPY --from=builder /temp/edgex-watermill-consumer /.

RUN ls /res

CMD [ "/edgex-watermill-consumer", "--confdir=/res" ]