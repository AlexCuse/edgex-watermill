# edgex-watermill example

This example uses AMQP between a sample producer and consumer to demonstrate edgex / watermill integration.

To start run 

`docker-compose up`

While the app is running you can send messages to the producer using the following command

`curl -X POST -H 'Content-Type: application/json' -d '{ "name": "test-name", "body": "some other string" }' http://localhost:59730/api/v2/trigger`