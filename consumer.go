package hopper

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumerQueue configures a queue that a consumer will fetch messages from.
type ConsumerQueue struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	AutoAck    bool

	QueueOptions    amqp.Table
	ConsumerOptions amqp.Table
	Bindings        map[string][]string
}

// Consumer manages consuming deliveries from the AMQP server.
type Consumer struct {
	id   string
	chnl *amqp.Channel

	msgs <-chan amqp.Delivery

	mtx *sync.Mutex

	closed   bool
	errDone  error
	chnlDone chan struct{}
}

// NewConsumer uses the controller's connection to create a new channel that
// consumes messages from a queue. The queue is declared and the bindings are
// added during the creation. A Consumer object is returned that handles
// consuming messages on the new channel.
func (ctrl *Controller) NewConsumer(
	id string,
	queue ConsumerQueue,
) (*Consumer, error) {
	chnl, err := ctrl.conn.Channel()
	if err != nil {
		err = fmt.Errorf("failed to create channel: %w", err)
		return nil, err
	}

	closeCh := make(chan *amqp.Error)
	chnl.NotifyClose(closeCh)

	// create queue
	_, err = chnl.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.AutoDelete,
		queue.Exclusive,
		false,
		queue.QueueOptions,
	)
	if err != nil {
		err = fmt.Errorf("failed to create queue: %w", err)
		return nil, err
	}

	// bind queue to exchanges
	for exchange, keys := range queue.Bindings {
		for _, key := range keys {
			err = chnl.QueueBind(queue.Name, key, exchange, false, nil)
			if err != nil {
				err = fmt.Errorf("failed to bind queue to exchange: %w", err)
				return nil, err
			}
		}
	}

	// create consumer
	msgs, err := chnl.Consume(
		queue.Name,
		id,
		queue.AutoAck,
		queue.Exclusive,
		false,
		false,
		queue.ConsumerOptions,
	)
	if err != nil {
		err = fmt.Errorf("failed to consume on channel: %w", err)
		return nil, err
	}

	c := &Consumer{
		id:       id,
		chnl:     chnl,
		msgs:     msgs,
		mtx:      &sync.Mutex{},
		closed:   false,
		errDone:  nil,
		chnlDone: make(chan struct{}),
	}

	// start close handler
	go func() {
		err := <-closeCh
		if err != nil {
			c.close(err)
		}
	}()

	return c, nil
}

// GetId gets the id of the consumer.
func (c *Consumer) GetId() string {
	return c.id
}

// Done returns a channel that notifies when the consumer is closed, either
// from calling shutdown or encountering an error.
func (c *Consumer) Done() <-chan struct{} {
	return c.chnlDone
}

// Error returns an error that describes the cause of the consumer closing from
// encountering an error.
func (c *Consumer) Error() error {
	return c.errDone
}

// Shutdown closes the consumer if it is not closed yet, then waits for the
// consumer to finish or untill the context expires.
func (c *Consumer) Shutdown(ctx context.Context) error {
	go c.close(nil)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.chnlDone:
		return nil
	}
}

// close sets the consumer to be closed and closes the done channel.
func (c *Consumer) close(err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if !c.closed {
		c.closed = true
		c.errDone = err

		c.chnl.Close()
		close(c.chnlDone)
	}
}

// Deliveries reaturns the channel to consume deliveries from.
func (rc *Consumer) Deliveries() <-chan amqp.Delivery {
	return rc.msgs
}

// Ack calls the same function on amqp.Channel in the consumer
func (rc *Consumer) Ack(tag uint64, multiple bool) error {
	return rc.chnl.Ack(tag, multiple)
}

// Nack calls the same function on amqp.Channel in the consumer
func (rc *Consumer) Nack(tag uint64, multiple bool, requeue bool) error {
	return rc.chnl.Nack(tag, multiple, requeue)
}

// Reject calls the same function on amqp.Channel in the consumer
func (rc *Consumer) Reject(tag uint64, multiple bool) error {
	return rc.chnl.Reject(tag, multiple)
}
