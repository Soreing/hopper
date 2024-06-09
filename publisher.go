package hopper

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublisherMiddleware describes a function that is called during the publish
// pipeline. The context matches the context the Publish function was called
// with and PublishingContext contains the publishing that can be modified,
// as well as a generic value storage.
type PublisherMiddleware func(context.Context, *PublishingContext)

// PublishingContext facilitates running middlewares on publishing, containing
// a modifiable publishing and a value storage. The Next() method moves the
// publishing pipeline to the next middleware.
type PublishingContext struct {
	pub *Publisher
	ctx context.Context

	Exchange   string
	RoutingKey string
	Publishing *amqp.Publishing

	mdwIdx  int
	mdwList []PublisherMiddleware

	mtx    *sync.RWMutex
	Values map[string]any

	Errors []error
}

// PublisherId gets the id of the publisher handling the publishing pipeline.
func (ctx *PublishingContext) PublisherId() string {
	return ctx.pub.id
}

// Lock locks the mutex within the context
func (ctx *PublishingContext) Lock() {
	ctx.mtx.Lock()
}

// Unlock unlocks the mutex within the context
func (ctx *PublishingContext) Unlock() {
	ctx.mtx.Unlock()
}

// Set assigns some value to a key in the context's Values store. The operation
// locks the context's mutex.
func (ctx *PublishingContext) Set(key string, val any) {
	ctx.mtx.Lock()
	ctx.Values[key] = val
	ctx.mtx.Unlock()
}

// Get retrieves some value from the context's Values store by a key. The
// operation locks the context's mutex.
func (ctx *PublishingContext) Get(key string) (val any, ok bool) {
	ctx.mtx.Lock()
	val, ok = ctx.Values[key]
	ctx.mtx.Unlock()
	return val, ok
}

// Next calls the next handler on the middleware chain, or calls publish when
// all middlewares have been called.
func (ctx *PublishingContext) Next() {
	idx := ctx.mdwIdx
	ctx.mdwIdx++

	if idx < len(ctx.mdwList) {
		ctx.mdwList[idx](ctx.ctx, ctx)

	} else if idx == len(ctx.mdwList) {
		ctx.pub.cnf.add(1)
		defer ctx.pub.cnf.done()

		dcnf, err := ctx.pub.publish(
			ctx.ctx,
			ctx.Exchange,
			ctx.RoutingKey,
			*ctx.Publishing,
		)
		if err != nil {
			ctx.Error(err)
		} else if ok, err := dcnf.WaitContext(ctx.ctx); err != nil {
			ctx.Error(err)
		} else if !ok {
			ctx.Error(fmt.Errorf("failed to confirm publishing"))
		}
	}

	ctx.mdwIdx--
}

// Error appends an error to the list of errors in the context. Only the first
// error will be returned from publish.
func (ctx *PublishingContext) Error(err error) {
	ctx.Errors = append(ctx.Errors, err)
}

// GetError returns the first error in the list or nil
func (ctx *PublishingContext) GetError() (err error) {
	if len(ctx.Errors) > 0 {
		return ctx.Errors[0]
	} else {
		return nil
	}
}

// Publisher manages publishing and confirmations to the AMQP server.
type Publisher struct {
	id   string
	chnl *amqp.Channel

	mtx *sync.Mutex
	cnf *waitCounter

	mdws []PublisherMiddleware

	closed   bool
	errDone  error
	chnlDone chan struct{}
}

// NewPublisher uses the controller's connection to create a new channel that
// listens to confirmations. A Publisher object is returned that handles
// publishing messages on the new channel.
func (ctrl *Controller) NewPublisher(
	id string,
) (*Publisher, error) {
	chnl, err := ctrl.conn.Channel()
	if err != nil {
		err = fmt.Errorf("failed to create channel: %w", err)
		return nil, err
	}

	confirmCh := make(chan amqp.Confirmation, 256)
	chnl.NotifyPublish(confirmCh)

	closeCh := make(chan *amqp.Error, 1)
	chnl.NotifyClose(closeCh)

	err = chnl.Confirm(false)
	if err != nil {
		err = fmt.Errorf("failed to put the channel in confirm mode: %w", err)
		return nil, err
	}

	p := &Publisher{
		id:       id,
		chnl:     chnl,
		mtx:      &sync.Mutex{},
		cnf:      newWaitCounter(),
		mdws:     []PublisherMiddleware{},
		closed:   false,
		errDone:  nil,
		chnlDone: make(chan struct{}),
	}

	// start confirms handler
	go func() {
		for ok := true; ok; {
			_, ok = <-confirmCh
		}
	}()

	// start close handler
	go func() {
		err := <-closeCh
		if err != nil {
			ctrl.close(err)
		}
	}()

	return p, nil
}

// GetId gets the id of the publisher.
func (p *Publisher) GetId() string {
	return p.id
}

// Done returns a channel that notifies when the publisher is closed, either
// from calling shutdown or encountering an error.
func (p *Publisher) Done() <-chan struct{} {
	return p.chnlDone
}

// Error returns an error that describes the cause of the publisher closing from
// encountering an error.
func (p *Publisher) Error() error {
	return p.errDone
}

// Shutdown closes the publisher if it is not closed yet, then waits for the
// publisher to finish or untill the context expires.
func (p *Publisher) Shutdown(ctx context.Context) error {
	go p.close(nil)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.chnlDone:
		return nil
	}
}

// close sets the publisher to be closed and waits for all pending
// confirmations to arrive before closing the done channel.
func (p *Publisher) close(err error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if !p.closed {
		p.closed = true
		p.errDone = err

		p.chnl.Close()
		p.cnf.wait()
		close(p.chnlDone)
	}
}

// Use adds middleware handlers on the publish pipeline that will be called in
// order before a message is published.
func (p *Publisher) Use(handlers ...PublisherMiddleware) {
	p.mdws = append(p.mdws, handlers...)
}

// PendingMessageCount returns the number of messages waiting confirmation.
func (p *Publisher) PendingMessageCount() int {
	return p.cnf.ctr
}

// Publish creates a message from the body and publishes it to the given
// exchange with the routing key. The publishing is asynchronous but it is a
// blocking operation until a confirmation is received.
func (p *Publisher) Publish(
	ctx context.Context,
	exchange string,
	routingKey string,
	contentType string,
	content []byte,
) error {
	pub := amqp.Publishing{
		ContentType: contentType,
		Body:        content,
	}

	// fast publish if there are no middlewares
	if len(p.mdws) == 0 {
		p.cnf.add(1)
		defer p.cnf.done()

		dcnf, err := p.publish(ctx, exchange, routingKey, pub)
		if err != nil {
			return err
		} else if ok := dcnf.Wait(); !ok {
			return fmt.Errorf("failed to confirm publishing")
		}
		return nil
	}

	// create publishing context and run middlewares
	mdws := make([]PublisherMiddleware, len(p.mdws))
	copy(mdws, p.mdws)

	pctx := PublishingContext{
		pub:        p,
		ctx:        ctx,
		Exchange:   exchange,
		RoutingKey: routingKey,
		Publishing: &pub,
		mdwIdx:     0,
		mdwList:    mdws,
		mtx:        &sync.RWMutex{},
		Values:     map[string]any{},
		Errors:     []error{},
	}

	pctx.Next()
	return pctx.GetError()
}

// PublishAsync calls publish and returns a channel that sends an error if the
// publishing failed or sends nil when the confirmation arrived.
func (p *Publisher) PublishAsync(
	ctx context.Context,
	exchange string,
	routingKey string,
	contentType string,
	content []byte,
) <-chan error {
	ch := make(chan error)

	go func() {
		ch <- p.Publish(ctx, exchange, routingKey, contentType, content)
		close(ch)
	}()

	return ch
}

// publish sends a publishing to some exchange with some routing key.
func (p *Publisher) publish(
	ctx context.Context,
	exchange string,
	routingKey string,
	pub amqp.Publishing,
) (*amqp.DeferredConfirmation, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.closed {
		return nil, fmt.Errorf("publisher is closed")
	}

	return p.chnl.PublishWithDeferredConfirmWithContext(
		ctx, exchange, routingKey, false, false, pub,
	)
}
