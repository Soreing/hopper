package hopper

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Controller has an AMQP connection and a channel.
type Controller struct {
	conn *amqp.Connection
	chnl *amqp.Channel

	mtx *sync.Mutex

	closed   bool
	errDone  error
	chnlDone chan struct{}
}

// NewController creates a controller by connecting to the AMQP server with a
// url and creating a channel.
func NewController(
	url string,
	config amqp.Config,
) (*Controller, error) {
	conn, err := amqp.DialConfig(url, config)
	if err != nil {
		err = fmt.Errorf("failed to create connection: %w", err)
		return nil, err
	}

	connCloseCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(connCloseCh)

	chnl, err := conn.Channel()
	if err != nil {
		err = fmt.Errorf("failed to create channel: %w", err)
		return nil, err
	}

	chnlCloseCh := make(chan *amqp.Error, 1)
	chnl.NotifyClose(chnlCloseCh)

	ctrl := &Controller{
		conn:     conn,
		chnl:     chnl,
		mtx:      &sync.Mutex{},
		closed:   false,
		errDone:  nil,
		chnlDone: make(chan struct{}),
	}

	// start connection close handler
	go func() {
		err := <-connCloseCh
		if err != nil {
			ctrl.close(err)
		}
	}()

	// start channel close handler
	go func() {
		err := <-chnlCloseCh
		if err != nil {
			ctrl.close(err)
		}
	}()

	return ctrl, nil
}

// Done returns a channel that notifies when the controller is closed, either
// from calling shutdown or encountering an error.
func (ctrl *Controller) Done() <-chan struct{} {
	return ctrl.chnlDone
}

// Error returns an error that describes the cause of the controller closing from
// encountering an error.
func (ctrl *Controller) Error() error {
	return ctrl.errDone
}

// Shutdown closes the controller if it is not closed yet.
func (ctrl *Controller) Shutdown(ctx context.Context) error {
	go ctrl.close(nil)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ctrl.chnlDone:
		return nil
	}
}

// close sets the controller to be closed and stops the amqp channel
// and connection associated with the controller before closing the done channel.
func (ctrl *Controller) close(err error) {
	ctrl.mtx.Lock()
	defer ctrl.mtx.Unlock()

	if !ctrl.closed {
		ctrl.closed = true
		ctrl.errDone = err

		ctrl.chnl.Close()
		ctrl.conn.Close()
		close(ctrl.chnlDone)
	}
}

// ExchangeDeclare calls the equivalent function using the amqp.Channel in
// the controller.
func (ctrl *Controller) ExchangeDeclare(
	name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table,
) error {
	return ctrl.chnl.ExchangeDeclare(
		name, kind, durable, autoDelete, internal, noWait, args,
	)
}

// ExchangeDeclarePassive calls the equivalent function using the amqp.Channel
// in the controller.
func (ctrl *Controller) ExchangeDeclarePassive(
	name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table,
) error {
	return ctrl.chnl.ExchangeDeclarePassive(
		name, kind, durable, autoDelete, internal, noWait, args,
	)
}

// ExchangeDelete calls the equivalent function using the amqp.Channel
// in the controller.
func (ctrl *Controller) ExchangeDelete(
	name string, ifUnused, noWait bool,
) error {
	return ctrl.chnl.ExchangeDelete(name, ifUnused, noWait)
}

// ExchangeBind calls the equivalent function using the amqp.Channel in
// the controller.
func (ctrl *Controller) ExchangeBind(
	destination, key, source string, noWait bool, args amqp.Table,
) error {
	return ctrl.chnl.ExchangeBind(destination, key, source, noWait, args)
}

// ExchangeUnbind calls the equivalent function using the amqp.Channel
// in the controller.
func (ctrl *Controller) ExchangeUnbind(
	destination, key, source string, noWait bool, args amqp.Table,
) error {
	return ctrl.chnl.ExchangeUnbind(destination, key, source, noWait, args)
}

// QueueDeclare calls the equivalent function using the amqp.Channel in
// the controller.
func (ctrl *Controller) QueueDeclare(
	name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table,
) (amqp.Queue, error) {
	return ctrl.chnl.QueueDeclare(
		name, durable, autoDelete, exclusive, noWait, args,
	)
}

// QueueDeclarePassive calls the equivalent function using the amqp.Channel
// in the controller.
func (ctrl *Controller) QueueDeclarePassive(
	name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table,
) (amqp.Queue, error) {
	return ctrl.chnl.QueueDeclarePassive(
		name, durable, autoDelete, exclusive, noWait, args,
	)
}

// QueueDelete calls the equivalent function using the amqp.Channel
// in the controller.
func (ctrl *Controller) QueueDelete(
	name string, ifUnused, ifEmpty, noWait bool,
) (int, error) {
	return ctrl.chnl.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

// QueueBind calls the equivalent function using the amqp.Channel in
// the controller.
func (ctrl *Controller) QueueBind(
	name, key, exchange string, noWait bool, args amqp.Table,
) error {
	return ctrl.chnl.QueueBind(name, key, exchange, noWait, args)
}

// QueueUnbind calls the equivalent function using the amqp.Channel
// in the controller.
func (ctrl *Controller) QueueUnbind(
	name, key, exchange string, args amqp.Table,
) error {
	return ctrl.chnl.QueueUnbind(name, key, exchange, args)
}

// QueuePurge calls the equivalent function using the amqp.Channel
// in the controller.
func (ctrl *Controller) QueuePurge(name string, noWait bool) (int, error) {
	return ctrl.chnl.QueuePurge(name, noWait)
}
