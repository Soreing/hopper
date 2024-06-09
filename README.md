![Build](https://github.com/soreing/hopper/actions/workflows/build_status.yaml/badge.svg)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/Soreing/4b6f950f01f3e6e5b9ed17b268664538/raw/hopper)
[![Go Report Card](https://goreportcard.com/badge/github.com/Soreing/hopper)](https://goreportcard.com/report/github.com/Soreing/hopper)
[![Go Reference](https://pkg.go.dev/badge/github.com/Soreing/hopper.svg)](https://pkg.go.dev/github.com/Soreing/hopper)

# Hopper
Hopper is a wrapper around the amqp091-go package, providing a simplified and quick 
way to create publishers and consumers on RabbitMQ.

## Controllers
Connect to the RabbitMQ server by creating a Controller instance with a connection 
URL and configs.
```golang
url := "amqp://guest:guest@localhost:5672/"
ctrl, err := hopper.NewController(url, amqp091.Config{})
if err != nil {
    panic(err)
}
```

The Controller implements functions that call the internal amqp091.Channel for 
interacting with exchanges and queues. The below example sets up a simple test
case, where a `messages` exchange routes inbound messages to the `inbound` queue.

```golang
ctrl.ExchangeDeclare("messages", "direct", true, false, false, false, nil)
ctrl.QueueDeclare("inbound", true, false, false, false, nil)
ctrl.QueueBind("inbound", "messages.inbound", "messages", false, nil)
```

## Publishers
The Controller can create Publishers, which create their own amqp091.Channel
and manage the publishing of messages. The publisher needs an Id to distinguish
different publishers.

```golang
pub, err := ctrl.NewPublisher("publisher-id")
if err != nil {
    panic(err)
}
```

Publishers support middleware functions that execute on every message that is 
sent with the publisher. The middleware function provides the original context
of the called function, as well as a PublisherContext, which exposes
the amqp091.Publishing object for modification, as well as the exchange 
and routing key.

Middleware functions execute in the order they were attached. To move to the
next function in the publishing pipeline, call .Next() on the publishing context.

```golang
pub.Use(func(ctx context.Context, pctx *hopper.PublishingContext) {
    pubId := pctx.PublisherId()
    msgId := "3e9dc427-a233-4352-878b-808312f7ec48"
    pctx.Publishing.MessageId = msgId

    start := time.Now()
    pctx.Next()
    elapsed := time.Since(start)

    if err := pctx.GetError(); err == nil {
        fmt.Println("Message", msgId, "delivered in", elapsed, "by", pubId)
    }
})
```

Publishing sends a message body with a content type to some exchange with a routing key. 
```golang
ctx := context.TODO()
message := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")

// Publishing synchronously (blocking)
err = pub.Publish(ctx, "messages", "messages.inbound", "plain/text", message)
if err != nil {
    panic(err)
}

// Publishing asynchronously (non-blocking)
err = pub.PublishAsync(ctx, "messages", "messages.inbound", "plain/text", message)
if err != nil {
    panic(err)
}
```

## Consumers
The Controller can create Consumers, which create their own amqp091.Channel
and manage the consuming of messages. The consumer needs an Id to distinguish
different consumers and a consumer channel to declare the queue with bindings.

```golang
queue := hopper.ConsumerQueue{
    Name:    "inbound",
    Durable: true,
    AutoAck: false,
    Bindings: map[string][]string{
        "messages": {"messages.inbound"},
    },
}

con, err := ctrl.NewConsumer("consumer-id", queue)
if err != nil {
    panic(err)
}
```

To consume messages, get the amqp091.Delivery channel from the consumer. You can
acknowledge or reject messages directly on the amqp091.Delivery or on the consumer
with the delivery tag.
```golang
msg := <-con.Deliveries()
fmt.Println(string(msg.Body))
msg.Ack(false)
```

## Termination
Controllers, Publishers and Consumers include a Done() method, returning a 
channel that closes when the entities are closed. When encountering an error,
the entities close automatically and the error can be fetched with the Error()
method. To gracefully shut down the entities, call Shutdown().
```golang
go func(con *hopper.Consumer) {
    <-con.Done()
    if err := con.Error(); err != nil {
        fmt.Println("Consumer terminated with error:", err)
    }
}(con)

con.Shutdown(ctx)
```