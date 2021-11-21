FROM golang:1.16-alpine AS builder

# add git for go modules
RUN apk update && apk add --no-cache make git gcc libc-dev libsodium-dev zeromq-dev

WORKDIR /temp

COPY go.mod go.mod

RUN go mod download

COPY . .
COPY ./res ./res

RUN go build -o ./edgex-watermill-producer ./...

RUN apk info -a zeromq-dev

# Next image - Copy built Go binary into new workspace
FROM alpine

RUN apk add --no-cache zeromq

RUN apk info -a zeromq

COPY --from=builder /temp/res /res
COPY --from=builder /temp/edgex-watermill-producer /.

RUN ls /res

CMD [ "/edgex-watermill-producer", "--confdir=/res" ]