FROM golang:1.16-alpine3.14 AS builder

# add git for go modules
RUN apk update && apk add --no-cache make git gcc libc-dev libsodium-dev zeromq-dev
WORKDIR /temp

COPY . .

RUN ls .

RUN go mod download

WORKDIR /temp/_example/edgex-watermill-consumer

RUN go build -o ./edgex-watermill-consumer ./...

RUN apk info -a zeromq-dev

# Next image - Copy built Go binary into new workspace
FROM alpine

RUN apk add --no-cache zeromq

RUN apk info -a zeromq

COPY --from=builder /temp/_example/edgex-watermill-consumer/res /res
COPY --from=builder /temp/_example/edgex-watermill-consumer/edgex-watermill-consumer /.

RUN ls /res

CMD [ "/edgex-watermill-consumer", "--confdir=/res" ]