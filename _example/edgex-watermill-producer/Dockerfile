FROM golang:1.16-alpine3.14 AS builder

# add git for go modules
RUN apk update && apk add --no-cache make git gcc libc-dev libsodium-dev zeromq-dev
WORKDIR /temp

COPY . .

WORKDIR /temp/_example/edgex-watermill-producer

RUN go mod download

RUN go build -o ./edgex-watermill-producer ./...

RUN apk info -a zeromq-dev

# Next image - Copy built Go binary into new workspace
FROM alpine

RUN apk add --no-cache zeromq

RUN apk info -a zeromq

COPY --from=builder /temp/_example/edgex-watermill-producer/res /res
COPY --from=builder /temp/_example/edgex-watermill-producer/edgex-watermill-producer /.

RUN ls /res

CMD [ "/edgex-watermill-producer", "--confdir=/res" ]